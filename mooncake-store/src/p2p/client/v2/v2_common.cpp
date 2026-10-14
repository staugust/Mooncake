#include "p2p/client/v2/v2_common.h"

#include <array>
#include <sstream>
#include <string>

#include <glog/logging.h>

namespace mooncake::v2 {

const char* ToString(AllocationSource source) {
    switch (source) {
        case AllocationSource::kPut:
            return "put";
        case AllocationSource::kPreWrite:
            return "prewrite";
        case AllocationSource::kWriteRemoteData:
            return "write_remote_data";
        case AllocationSource::kMigration:
            return "migration";
        case AllocationSource::kOnboard:
            return "onboard";
        case AllocationSource::kCount:
            break;
    }
    return "unknown";
}

const char* ToString(LifecycleState state) {
    switch (state) {
        case LifecycleState::kRunning:
            return "running";
        case LifecycleState::kStopping:
            return "stopping";
        case LifecycleState::kStopped:
            return "stopped";
        case LifecycleState::kDestroyed:
            return "destroyed";
    }
    return "unknown";
}

namespace {
// Upper bound on synchronous reclaim rounds. More than this on the request
// path turns an allocation failure into a latency cliff.
constexpr uint32_t kMaxEvictRounds = 8;
// Upper bound on the synchronous reclaim deadline, for the same reason.
constexpr std::chrono::milliseconds kMaxEvictTimeout{1000};
}  // namespace

tl::expected<void, ErrorCode> ValidateAllocationFailurePolicy(
    const AllocationFailurePolicyConfig& config) {
    if (!config.try_evict) {
        // The remaining fields are unused when eviction is disabled, so they
        // are deliberately not validated: a stale value cannot change
        // behaviour.
        return {};
    }
    if (config.max_evict_rounds == 0 ||
        config.max_evict_rounds > kMaxEvictRounds) {
        LOG(ERROR) << "allocation_failure.max_evict_rounds must be in [1, "
                   << kMaxEvictRounds << "], got " << config.max_evict_rounds;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.evict_timeout <= std::chrono::milliseconds::zero() ||
        config.evict_timeout > kMaxEvictTimeout) {
        LOG(ERROR) << "allocation_failure.evict_timeout_ms must be in (0, "
                   << kMaxEvictTimeout.count() << "], got "
                   << config.evict_timeout.count();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

// ---------------------------------------------------------------------------
// AllocationFailureMetrics
// ---------------------------------------------------------------------------

AllocationFailureMetrics::AllocationFailureMetrics(
    const std::vector<UUID>& tiler_ids) {
    // Every slot is created here and never again, which is what lets the
    // request path treat this as read-only.
    counters_.reserve(tiler_ids.size());
    for (const auto& id : tiler_ids) {
        counters_.try_emplace(id);
    }
}

AllocationFailureCounters& AllocationFailureMetrics::For(
    const UUID& tiler_id, AllocationSource source) {
    const size_t index = static_cast<size_t>(source);
    auto it = counters_.find(tiler_id);
    if (it == counters_.end()) {
        if (!warned_unknown_.exchange(true, std::memory_order_relaxed)) {
            LOG(ERROR) << "Allocation failure recorded for an unregistered "
                          "tier; counting it in the shared unknown slot";
        }
        return unknown_[index];
    }
    return it->second[index];
}

std::string AllocationFailureMetrics::Serialize() const {
    static constexpr std::array<const char*, 6> kOutcomes = {
        "failures",        "evict_disabled", "evict_attempted",
        "retry_succeeded", "retry_failed",   "evict_timed_out"};

    std::ostringstream stream;
    stream << "# HELP mooncake_p2p_v2_allocation_failure_outcomes_total "
              "Allocation failures and the synchronous reclaim outcomes for "
              "each tier/source\n";
    stream << "# TYPE mooncake_p2p_v2_allocation_failure_outcomes_total "
              "counter\n";

    stream << "# HELP mooncake_p2p_v2_allocation_failure_last_size_bytes "
              "Size requested by the latest allocation failure\n";
    stream << "# TYPE mooncake_p2p_v2_allocation_failure_last_size_bytes "
              "gauge\n";
    stream << "# HELP mooncake_p2p_v2_allocation_failure_last_free_bytes "
              "Free bytes seen by the latest allocation failure\n";
    stream << "# TYPE mooncake_p2p_v2_allocation_failure_last_free_bytes "
              "gauge\n";

    auto emit = [&stream](const std::string& tier_label,
                          const AllocationFailureCounters& counters,
                          AllocationSource source) {
        const std::array<uint64_t, 6> values = {
            counters.failures.load(std::memory_order_relaxed),
            counters.evict_disabled.load(std::memory_order_relaxed),
            counters.evict_attempted.load(std::memory_order_relaxed),
            counters.retry_succeeded.load(std::memory_order_relaxed),
            counters.retry_failed.load(std::memory_order_relaxed),
            counters.evict_timed_out.load(std::memory_order_relaxed)};
        for (size_t outcome_index = 0; outcome_index < kOutcomes.size();
             ++outcome_index) {
            if (values[outcome_index] == 0) continue;
            stream << "mooncake_p2p_v2_allocation_failure_outcomes_total"
                      "{tier=\""
                   << tier_label << "\",source=\"" << ToString(source)
                   << "\",outcome=\"" << kOutcomes[outcome_index] << "\"} "
                   << values[outcome_index] << "\n";
        }

        const uint64_t last_size =
            counters.last_size_bytes.load(std::memory_order_relaxed);
        if (last_size != 0) {
            stream << "mooncake_p2p_v2_allocation_failure_last_size_bytes"
                      "{tier=\""
                   << tier_label << "\",source=\"" << ToString(source) << "\"} "
                   << last_size << "\n";
        }
        const uint64_t last_free =
            counters.last_free_bytes.load(std::memory_order_relaxed);
        if (last_size != 0) {
            stream << "mooncake_p2p_v2_allocation_failure_last_free_bytes"
                      "{tier=\""
                   << tier_label << "\",source=\"" << ToString(source) << "\"} "
                   << last_free << "\n";
        }
    };

    for (const auto& [tiler_id, source_counters] : counters_) {
        std::ostringstream tier;
        tier << tiler_id;
        for (size_t source_index = 0;
             source_index < static_cast<size_t>(AllocationSource::kCount);
             ++source_index) {
            emit(tier.str(), source_counters[source_index],
                 static_cast<AllocationSource>(source_index));
        }
    }
    for (size_t source_index = 0;
         source_index < static_cast<size_t>(AllocationSource::kCount);
         ++source_index) {
        emit("unknown", unknown_[source_index],
             static_cast<AllocationSource>(source_index));
    }
    return stream.str();
}

// ---------------------------------------------------------------------------
// LifecycleGate::Guard
// ---------------------------------------------------------------------------

LifecycleGate::Guard::Guard(Guard&& other) noexcept
    : owner_(std::move(other.owner_)) {
    other.owner_.reset();
}

LifecycleGate::Guard& LifecycleGate::Guard::operator=(Guard&& other) noexcept {
    if (this != &other) {
        if (owner_ != nullptr) {
            owner_->Release();
        }
        owner_ = std::move(other.owner_);
        other.owner_.reset();
    }
    return *this;
}

LifecycleGate::Guard::~Guard() {
    if (owner_ != nullptr) {
        owner_->Release();
        owner_.reset();
    }
}

bool LifecycleGate::Guard::IsUsable() const {
    return owner_ != nullptr && !owner_->IsCancelled();
}

// ---------------------------------------------------------------------------
// LifecycleGate
// ---------------------------------------------------------------------------

tl::expected<LifecycleGate::Guard, ErrorCode> LifecycleGate::Acquire() {
    // Count first, then re-check: a Stop() that slips in between is caught by
    // the second read, and the guard we hand back is already accounted for so
    // Stop cannot conclude "no in-flight work" while we are still here.
    inflight_.fetch_add(1, std::memory_order_acq_rel);
    if (state_.load(std::memory_order_acquire) != LifecycleState::kRunning) {
        Release();
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return Guard(shared_from_this());
}

void LifecycleGate::Release() {
    const uint64_t previous = inflight_.fetch_sub(1, std::memory_order_acq_rel);
    DCHECK_GT(previous, 0U) << "LifecycleGate in-flight counter underflow";
    if (previous == 1) {
        // Only a waiting Stop() cares, and it holds wait_mu_ around its
        // predicate check, so taking it here closes the lost-wakeup window.
        std::lock_guard<std::mutex> lock(wait_mu_);
        wait_cv_.notify_all();
    }
}

bool LifecycleGate::BeginStop() {
    LifecycleState expected = LifecycleState::kRunning;
    return state_.compare_exchange_strong(expected, LifecycleState::kStopping,
                                          std::memory_order_acq_rel);
}

bool LifecycleGate::WaitForNoInflight(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(wait_mu_);
    return wait_cv_.wait_for(lock, timeout, [this] {
        return inflight_.load(std::memory_order_acquire) == 0;
    });
}

void LifecycleGate::BeginCancel() {
    cancelled_.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lock(wait_mu_);
    wait_cv_.notify_all();
}

bool LifecycleGate::IsCancelled() const {
    return cancelled_.load(std::memory_order_acquire);
}

void LifecycleGate::MarkStopped() {
    LifecycleState expected = LifecycleState::kStopping;
    state_.compare_exchange_strong(expected, LifecycleState::kStopped,
                                   std::memory_order_acq_rel);
}

bool LifecycleGate::MarkDestroyed() {
    LifecycleState previous =
        state_.exchange(LifecycleState::kDestroyed, std::memory_order_acq_rel);
    return previous != LifecycleState::kDestroyed;
}

LifecycleState LifecycleGate::State() const {
    return state_.load(std::memory_order_acquire);
}

uint64_t LifecycleGate::InflightForTest() const {
    return inflight_.load(std::memory_order_acquire);
}

}  // namespace mooncake::v2
