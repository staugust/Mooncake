#include "p2p/client/v2/movement_consumers.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <sstream>
#include <utility>

#include <glog/logging.h>

namespace mooncake::v2 {
namespace {

std::string UuidToString(const UUID& uuid) {
    std::ostringstream stream;
    stream << uuid;
    return stream.str();
}

std::string RouteDestinationLabel(const std::optional<UUID>& destination) {
    return destination.has_value() ? UuidToString(*destination) : "none";
}

OffloadScanOutcome OutcomeFor(PlacementRejectionReason reason) {
    switch (reason) {
        case PlacementRejectionReason::kSourceMissing:
            return OffloadScanOutcome::kPlacementSourceMissing;
        case PlacementRejectionReason::kNoDestination:
            return OffloadScanOutcome::kPlacementNoDestination;
        case PlacementRejectionReason::kDestinationOverLimit:
            return OffloadScanOutcome::kPlacementDestinationOverLimit;
        case PlacementRejectionReason::kAlreadyPresent:
            return OffloadScanOutcome::kPlacementDestinationAlreadyPresent;
        case PlacementRejectionReason::kDestinationNotAddressable:
            return OffloadScanOutcome::kPlacementDestinationNotAddressable;
        case PlacementRejectionReason::kMissingEdge:
            return OffloadScanOutcome::kPlacementMissingEdge;
        case PlacementRejectionReason::kNoAdmission:
            return OffloadScanOutcome::kPlacementNoAdmission;
    }
    return OffloadScanOutcome::kPlacementNoAdmission;
}

OffloadScanOutcome OutcomeFor(MovementRejection rejection) {
    switch (rejection) {
        case MovementRejection::kInflight:
            return OffloadScanOutcome::kTrackerRejectedInflight;
        case MovementRejection::kCooldown:
            return OffloadScanOutcome::kTrackerRejectedCooldown;
        case MovementRejection::kMinimumResidency:
            return OffloadScanOutcome::kTrackerRejectedMinimumResidency;
        case MovementRejection::kStopped:
            return OffloadScanOutcome::kTrackerRejectedStopped;
        case MovementRejection::kInvalid:
            return OffloadScanOutcome::kTrackerRejectedInvalid;
    }
    return OffloadScanOutcome::kTrackerRejectedInvalid;
}

/** Usage as a fraction of capacity; 0 for a tier with no capacity. */
double UsageRatioOf(const TilerManager& tiler) {
    const size_t capacity = tiler.Capacity();
    if (capacity == 0) return 0.0;
    return static_cast<double>(tiler.Usage()) / static_cast<double>(capacity);
}

/**
 * @brief Build the placement context.
 *
 * The two callbacks are the whole reason the policy can be stateless: it asks
 * for live usage and live presence at the moment it decides, instead of
 * holding a copy that was true when some earlier event arrived.
 */
PlacementContext MakeContext(const MovementConsumerDeps& deps,
                             std::string_view key, const UUID& source,
                             MovementDirection direction, size_t size_bytes,
                             double frequency,
                             const BlockRegistrationHandle& registration,
                             OffloadScanStats* scan_stats = nullptr) {
    PlacementContext context;
    context.key = key;
    context.source_tiler = source;
    context.direction = direction;
    context.size_bytes = size_bytes;
    context.frequency = frequency;
    context.usage_ratio = [deps](const UUID& tiler_id) -> double {
        TilerManager* tiler = deps.tilers->Find(tiler_id);
        return tiler == nullptr ? 1.0 : UsageRatioOf(*tiler);
    };
    context.already_present = [deps, &registration](const UUID& tiler_id) {
        TilerManager* tiler = deps.tilers->Find(tiler_id);
        // Only the destination's own index can answer this. A cached answer
        // would be a second source of truth about what exists, and the
        // executor re-checks it under the guard anyway -- this pre-check only
        // avoids proposing a copy that is certainly redundant.
        return tiler != nullptr && tiler->Match(registration).has_value();
    };
    if (scan_stats != nullptr) {
        context.on_reject = [scan_stats,
                             source](const PlacementRejection& rejection) {
            scan_stats->Record(source, rejection.destination_tiler,
                               OutcomeFor(rejection.reason));
        };
    }
    return context;
}

enum class ProposalOutcome : uint8_t {
    kEnqueued,
    kTrackerRejected,
    kSinkRejected,
};

struct ProposalResult {
    ProposalOutcome outcome = ProposalOutcome::kEnqueued;
    std::optional<MovementRejection> tracker_rejection;
};

ConsumeResult ToConsumeResult(ProposalOutcome outcome) {
    return outcome == ProposalOutcome::kEnqueued
               ? ConsumeResult::kCommandEnqueued
               : ConsumeResult::kIgnored;
}

/** Claim, build and hand over one command, preserving the rejection reason. */
ProposalResult Propose(const MovementConsumerDeps& deps,
                       const MovementConsumerConfig& config,
                       const PlacementDecision& decision,
                       const BlockEvent& event,
                       const BlockRegistrationHandle& registration,
                       const BlockId& block_id, MovementDirection direction,
                       OffloadScanStats* scan_stats = nullptr) {
    MovementDedupKey dedup;
    dedup.registration_id = registration.Id();
    dedup.source_block_id = block_id;
    dedup.source_tiler = event.tiler_id;
    dedup.destination_tiler = decision.destination_tiler;

    auto lease = deps.movement->TryAcquire(event.key, dedup, direction);
    if (!lease) {
        // In flight, cooling down or too freshly arrived. All three are the
        // policy working, not a failure, so the fact was still applied.
        if (scan_stats != nullptr) {
            scan_stats->Record(event.tiler_id, decision.destination_tiler,
                               OutcomeFor(lease.error()));
        }
        return {ProposalOutcome::kTrackerRejected, lease.error()};
    }

    MovementRequest request;
    request.kind = decision.kind;
    request.key = event.key;
    request.registration = registration.Downgrade();
    request.source_tiler = event.tiler_id;
    request.destination_tiler = decision.destination_tiler;
    request.source_block_id = block_id;
    request.route = decision.route;
    request.length = event.size_bytes;
    // Neither direction has a caller blocked on it: offload is speculative
    // capacity work and onboard is speculative warming. Reclamation is what
    // runs in the foreground, and it does not travel this pipeline at all.
    request.priority = MovementPriority::kBackground;
    request.deadline = deps.clock->Now() + config.movement_deadline;

    if (scan_stats != nullptr) {
        scan_stats->Record(event.tiler_id, decision.destination_tiler,
                           OffloadScanOutcome::kTrackerAccepted);
    }
    if (!deps.sink->Enqueue(std::move(request), std::move(lease.value()))) {
        if (scan_stats != nullptr) {
            scan_stats->Record(event.tiler_id, decision.destination_tiler,
                               OffloadScanOutcome::kQueueRejected);
        }
        return {ProposalOutcome::kSinkRejected, std::nullopt};
    }
    if (scan_stats != nullptr) {
        scan_stats->Record(event.tiler_id, decision.destination_tiler,
                           OffloadScanOutcome::kEnqueued);
    }
    return {ProposalOutcome::kEnqueued, std::nullopt};
}

/**
 * @brief Scan one source tiler for up to offload_batch_size cold victims.
 *
 * This is the shared scanner core used by both the event consumer and the
 * dedicated offload threads; the source tiler is the only input necessary,
 * because victim selection is a property of that tiler, not of the commit
 * event that may have noticed it.
 */
ConsumeResult OffloadFromTilerImpl(const MovementConsumerConfig& config,
                                   const MovementConsumerDeps& deps,
                                   const UUID& tiler_id, size_t lane_count,
                                   size_t lane_index) {
    OffloadScanStats* scan_stats = deps.scan_stats;
    if (scan_stats != nullptr) {
        scan_stats->Record(tiler_id, std::nullopt,
                           OffloadScanOutcome::kScanPasses);
    }
    TilerManager* source = deps.tilers->Find(tiler_id);
    if (source == nullptr) {
        if (scan_stats != nullptr) {
            scan_stats->Record(tiler_id, std::nullopt,
                               OffloadScanOutcome::kScanSourceMissing);
        }
        return ConsumeResult::kIgnored;
    }
    if (UsageRatioOf(*source) < config.offload_high_watermark) {
        if (scan_stats != nullptr) {
            scan_stats->Record(tiler_id, std::nullopt,
                               OffloadScanOutcome::kScanBelowWatermark);
        }
        return ConsumeResult::kIgnored;
    }

    if (lane_count == 0 || lane_index >= lane_count) {
        if (scan_stats != nullptr) {
            scan_stats->Record(tiler_id, std::nullopt,
                               OffloadScanOutcome::kScanLaneInvalid);
        }
        return ConsumeResult::kIgnored;
    }

    // For a single lane this is the event-driven batch. For a scanner pool it
    // is the pool-sized candidate set, which the pool then partitions into
    // disjoint lanes. That avoids N scanners all contending on the same first
    // victim and turning a thread pool into a retry storm.
    size_t max_keys = config.offload_batch_size;
    if (lane_count > 1 && config.offload_batch_size >
                              std::numeric_limits<size_t>::max() / lane_count) {
        if (scan_stats != nullptr) {
            scan_stats->Record(tiler_id, std::nullopt,
                               OffloadScanOutcome::kScanBatchOverflow);
        }
        return ConsumeResult::kIgnored;
    }
    if (lane_count > 1) max_keys *= lane_count;

    auto victims = source->Eviction()->SelectVictims(
        std::numeric_limits<size_t>::max(), max_keys);
    if (!victims.empty() && scan_stats != nullptr) {
        scan_stats->Record(tiler_id, std::nullopt,
                           OffloadScanOutcome::kScanCandidatesSelected,
                           victims.size());
    }
    if (victims.empty()) {
        if (scan_stats != nullptr) {
            scan_stats->Record(tiler_id, std::nullopt,
                               OffloadScanOutcome::kScanNoCandidates);
        }
        return ConsumeResult::kIgnored;
    }

    ConsumeResult result = ConsumeResult::kIgnored;
    size_t candidate_index = 0;
    for (const auto& victim : victims) {
        const bool mine =
            lane_count == 1 || candidate_index % lane_count == lane_index;
        ++candidate_index;
        if (!mine) {
            if (scan_stats != nullptr) {
                scan_stats->Record(tiler_id, std::nullopt,
                                   OffloadScanOutcome::kScanCandidateNotOwned);
            }
            continue;
        }
        if (scan_stats != nullptr) {
            scan_stats->Record(tiler_id, std::nullopt,
                               OffloadScanOutcome::kScanCandidateExamined);
        }
        auto registration = victim.registration.Lock();
        if (!registration.has_value() || registration->IsRetired()) {
            if (scan_stats != nullptr) {
                scan_stats->Record(tiler_id, std::nullopt,
                                   OffloadScanOutcome::kScanCandidateStale);
            }
            continue;
        }
        if (!deps.registry->IsCanonical(*registration)) {
            if (scan_stats != nullptr) {
                scan_stats->Record(
                    tiler_id, std::nullopt,
                    OffloadScanOutcome::kScanCandidateNotCanonical);
            }
            continue;
        }

        const FrequencySnapshot heat =
            deps.frequency->Get(registration->Id(), victim.key);

        BlockEvent subject;
        subject.type = EventType::kCommit;
        subject.key = victim.key;
        subject.tiler_id = tiler_id;
        subject.size_bytes = victim.size_bytes;

        const PlacementContext context = MakeContext(
            deps, victim.key, tiler_id, MovementDirection::kOffload,
            victim.size_bytes, heat.read_heat, *registration, scan_stats);
        auto decision = deps.placement->Select(context);
        if (!decision.has_value()) {
            // The callback already recorded the no-destination reason when the
            // graph had no candidates, and the terminal no-admission event
            // when every candidate was rejected. Do not count it twice.
            continue;
        }
        if (scan_stats != nullptr) {
            const OffloadScanOutcome outcome =
                decision->kind == MovementKind::kSourceDetach
                    ? OffloadScanOutcome::kPlacementSourceDetachSelected
                    : OffloadScanOutcome::kPlacementSelected;
            scan_stats->Record(tiler_id, decision->destination_tiler, outcome);
        }

        const ProposalResult proposal =
            Propose(deps, config, *decision, subject, *registration,
                    victim.block_id, MovementDirection::kOffload, scan_stats);
        if (proposal.outcome == ProposalOutcome::kEnqueued) {
            result = ConsumeResult::kCommandEnqueued;
        }
    }
    return result;
}

/**
 * @class OffloadConsumer
 */
class OffloadConsumer final : public EventConsumer {
   public:
    OffloadConsumer(const MovementConsumerConfig& config,
                    const MovementConsumerDeps& deps)
        : config_(config), deps_(deps) {}

    Subscription SubscriptionInfo() const override {
        Subscription subscription;
        subscription.name = "offload";
        subscription.event_mask = SubscriptionMask({EventType::kCommit});
        return subscription;
    }

    ConsumeResult Consume(const BlockEvent& event, DeliveryMode mode) override {
        // Inline means a writer's thread. Offload is speculative; dropping the
        // decision costs a delayed copy, while making it here would put a
        // queue push into the write path.
        if (mode == DeliveryMode::kInline) return ConsumeResult::kApplied;
        return OffloadFromTilerImpl(config_, deps_, event.tiler_id,
                                    /*lane_count=*/1, /*lane_index=*/0);
    }

   private:
    MovementConsumerConfig config_;
    MovementConsumerDeps deps_;
};

/**
 * @class OnboardConsumer
 */
class OnboardConsumer final : public EventConsumer {
   public:
    OnboardConsumer(const MovementConsumerConfig& config,
                    const MovementConsumerDeps& deps)
        : config_(config), deps_(deps) {}

    Subscription SubscriptionInfo() const override {
        Subscription subscription;
        subscription.name = "onboard";
        subscription.event_mask = SubscriptionMask({EventType::kAccess});
        return subscription;
    }

    ConsumeResult Consume(const BlockEvent& event, DeliveryMode mode) override {
        if (mode == DeliveryMode::kInline) return ConsumeResult::kApplied;
        if (config_.onboard_min_read_heat <= 0.0) {
            return ConsumeResult::kIgnored;  // onboarding disabled
        }
        if (!event.registration.has_value() || !event.block_id.has_value()) {
            return ConsumeResult::kIgnored;
        }

        auto registration = event.registration->Lock();
        if (!registration.has_value() || registration->IsRetired()) {
            return ConsumeResult::kIgnored;
        }
        if (!deps_.registry->IsCanonical(*registration)) {
            return ConsumeResult::kIgnored;
        }

        // From the tracker, and from its read-only counter. The old policy
        // read a sketch that commits also bumped, so writing a key counted
        // towards "this key is in demand".
        const FrequencySnapshot heat =
            deps_.frequency->Get(registration->Id(), event.key);
        if (heat.read_heat < config_.onboard_min_read_heat) {
            return ConsumeResult::kIgnored;
        }

        const PlacementContext context = MakeContext(
            deps_, event.key, event.tiler_id, MovementDirection::kOnboard,
            event.size_bytes, heat.read_heat, *registration);
        auto decision = deps_.placement->Select(context);
        if (!decision.has_value()) return ConsumeResult::kIgnored;

        const ProposalResult proposal =
            Propose(deps_, config_, *decision, event, *registration,
                    *event.block_id, MovementDirection::kOnboard);
        return ToConsumeResult(proposal.outcome);
    }

   private:
    MovementConsumerConfig config_;
    MovementConsumerDeps deps_;
};

tl::expected<void, ErrorCode> ValidateDeps(const MovementConsumerDeps& deps) {
    if (deps.tilers == nullptr || deps.registry == nullptr ||
        deps.placement == nullptr || deps.frequency == nullptr ||
        deps.movement == nullptr || deps.sink == nullptr ||
        deps.clock == nullptr) {
        LOG(ERROR) << "A movement consumer needs tilers, a registry, a "
                      "placement policy, both trackers, a sink and a clock";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

}  // namespace

const char* ToString(OffloadScanOutcome outcome) {
    switch (outcome) {
        case OffloadScanOutcome::kScanPasses:
            return "scan_passes";
        case OffloadScanOutcome::kScanSourceMissing:
            return "scan_source_tier_missing";
        case OffloadScanOutcome::kScanBelowWatermark:
            return "scan_below_watermark";
        case OffloadScanOutcome::kScanLaneInvalid:
            return "scan_lane_invalid";
        case OffloadScanOutcome::kScanBatchOverflow:
            return "scan_batch_overflow";
        case OffloadScanOutcome::kScanNoCandidates:
            return "scan_no_candidates";
        case OffloadScanOutcome::kScanCandidatesSelected:
            return "scan_candidates_selected";
        case OffloadScanOutcome::kScanCandidateNotOwned:
            return "scan_candidate_not_owned";
        case OffloadScanOutcome::kScanCandidateExamined:
            return "scan_candidate_examined";
        case OffloadScanOutcome::kScanCandidateStale:
            return "scan_candidate_stale";
        case OffloadScanOutcome::kScanCandidateNotCanonical:
            return "scan_candidate_not_canonical";
        case OffloadScanOutcome::kPlacementSourceMissing:
            return "placement_source_missing";
        case OffloadScanOutcome::kPlacementNoDestination:
            return "placement_no_destination";
        case OffloadScanOutcome::kPlacementDestinationOverLimit:
            return "placement_destination_over_limit";
        case OffloadScanOutcome::kPlacementDestinationAlreadyPresent:
            return "placement_destination_already_present";
        case OffloadScanOutcome::kPlacementDestinationNotAddressable:
            return "placement_destination_not_addressable";
        case OffloadScanOutcome::kPlacementMissingEdge:
            return "placement_missing_edge";
        case OffloadScanOutcome::kPlacementNoAdmission:
            return "placement_no_admission";
        case OffloadScanOutcome::kPlacementSourceDetachSelected:
            return "placement_source_detach_selected";
        case OffloadScanOutcome::kPlacementSelected:
            return "placement_selected";
        case OffloadScanOutcome::kTrackerRejectedInflight:
            return "tracker_rejected_inflight";
        case OffloadScanOutcome::kTrackerRejectedCooldown:
            return "tracker_rejected_cooldown";
        case OffloadScanOutcome::kTrackerRejectedMinimumResidency:
            return "tracker_rejected_minimum_residency";
        case OffloadScanOutcome::kTrackerRejectedStopped:
            return "tracker_rejected_stopped";
        case OffloadScanOutcome::kTrackerRejectedInvalid:
            return "tracker_rejected_invalid";
        case OffloadScanOutcome::kTrackerAccepted:
            return "tracker_accepted";
        case OffloadScanOutcome::kQueueRejected:
            return "queue_rejected";
        case OffloadScanOutcome::kEnqueued:
            return "enqueued";
    }
    return "unknown";
}

OffloadScanStats::RouteKey OffloadScanStats::MakeKey(
    const UUID& source_tiler, const std::optional<UUID>& destination_tiler) {
    RouteKey key;
    key.source_tiler = source_tiler;
    key.has_destination = destination_tiler.has_value();
    if (destination_tiler.has_value()) {
        key.destination_tiler = *destination_tiler;
    }
    return key;
}

size_t OffloadScanStats::RouteKeyHash::operator()(
    const RouteKey& key) const noexcept {
    const auto first =
        static_cast<uint64_t>(key.source_tiler.first ^ key.source_tiler.second);
    const auto second =
        key.has_destination
            ? static_cast<uint64_t>(key.destination_tiler.first ^
                                    key.destination_tiler.second)
            : 0;
    return first ^ (second << 1) ^
           (key.has_destination ? 0x9e3779b97f4a7c15ULL : 0);
}

void OffloadScanStats::Record(const UUID& source_tiler,
                              const std::optional<UUID>& destination_tiler,
                              OffloadScanOutcome outcome, uint64_t count) {
    if (count == 0) return;
    const RouteKey key = MakeKey(source_tiler, destination_tiler);
    std::lock_guard<std::shared_mutex> lock(mutex_);
    auto& counters = routes_[key];
    counters.counters[static_cast<size_t>(outcome)].fetch_add(
        count, std::memory_order_relaxed);
}

uint64_t OffloadScanStats::Value(const UUID& source_tiler,
                                 const std::optional<UUID>& destination_tiler,
                                 OffloadScanOutcome outcome) const {
    const RouteKey key = MakeKey(source_tiler, destination_tiler);
    std::shared_lock<std::shared_mutex> lock(mutex_);
    const auto it = routes_.find(key);
    if (it == routes_.end()) return 0;
    return it->second.counters[static_cast<size_t>(outcome)].load(
        std::memory_order_relaxed);
}

std::string OffloadScanStats::Serialize() const {
    std::ostringstream stream;
    stream << "# HELP mooncake_p2p_offload_scan_decisions_total "
              "Offload scan decisions by source, destination and outcome\n";
    stream << "# TYPE mooncake_p2p_offload_scan_decisions_total counter\n";
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        for (size_t outcome_index = 0; outcome_index < kOffloadScanOutcomeCount;
             ++outcome_index) {
            const auto outcome = static_cast<OffloadScanOutcome>(outcome_index);
            for (const auto& [key, counters] : routes_) {
                const std::optional<UUID> destination =
                    key.has_destination
                        ? std::optional<UUID>(key.destination_tiler)
                        : std::nullopt;
                stream << "mooncake_p2p_offload_scan_decisions_total"
                          "{source_tier=\""
                       << UuidToString(key.source_tiler)
                       << "\",destination_tier=\""
                       << RouteDestinationLabel(destination) << "\",outcome=\""
                       << ToString(outcome) << "\"} "
                       << counters.counters[outcome_index].load(
                              std::memory_order_relaxed)
                       << "\n";
            }
        }
    }
    return stream.str();
}

tl::expected<void, ErrorCode> ValidateMovementConsumerConfig(
    const MovementConsumerConfig& config) {
    if (!(config.offload_high_watermark > 0.0) ||
        !(config.offload_high_watermark < 1.0)) {
        LOG(ERROR) << "movement.offload_high_watermark must be in (0, 1), got "
                   << config.offload_high_watermark
                   << "; offload has to start before the tier is full or it "
                      "cannot keep ahead of reclamation";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.offload_batch_size == 0) {
        LOG(ERROR) << "movement.offload_batch_size must be greater than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.offload_scan_interval <= std::chrono::milliseconds(0)) {
        LOG(ERROR) << "movement.offload_scan_interval_ms must be greater "
                      "than zero when offload scan threads are enabled";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.onboard_min_read_heat < 0.0) {
        LOG(ERROR) << "movement.onboard_min_read_heat must not be negative";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.movement_deadline <= std::chrono::milliseconds::zero()) {
        LOG(ERROR) << "movement.movement_deadline_ms must be greater than "
                      "zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<std::unique_ptr<EventConsumer>, ErrorCode> CreateOffloadConsumer(
    const MovementConsumerConfig& config, const MovementConsumerDeps& deps) {
    auto valid = ValidateMovementConsumerConfig(config);
    if (!valid) return tl::make_unexpected(valid.error());
    auto deps_valid = ValidateDeps(deps);
    if (!deps_valid) return tl::make_unexpected(deps_valid.error());
    return std::make_unique<OffloadConsumer>(config, deps);
}

tl::expected<std::unique_ptr<EventConsumer>, ErrorCode> CreateOnboardConsumer(
    const MovementConsumerConfig& config, const MovementConsumerDeps& deps) {
    auto valid = ValidateMovementConsumerConfig(config);
    if (!valid) return tl::make_unexpected(valid.error());
    auto deps_valid = ValidateDeps(deps);
    if (!deps_valid) return tl::make_unexpected(deps_valid.error());
    return std::make_unique<OnboardConsumer>(config, deps);
}

ConsumeResult OffloadFromTiler(const MovementConsumerConfig& config,
                               const MovementConsumerDeps& deps,
                               const UUID& tiler_id, size_t lane_count,
                               size_t lane_index) {
    return OffloadFromTilerImpl(config, deps, tiler_id, lane_count, lane_index);
}

}  // namespace mooncake::v2
