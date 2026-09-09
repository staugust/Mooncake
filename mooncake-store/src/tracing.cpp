// Copyright The Mooncake Authors. SPDX-License-Identifier: Apache-2.0
//
// OpenTelemetry tracing integration for mooncake store binaries.
//
// When MOONCAKE_ENABLE_OTEL_TRACING is defined the spans are exported over
// OTLP/HTTP to the collector given via --otlp-traces-endpoint, using the
// stock curl-based HttpClient shipped with opentelemetry-cpp (built with
// WITH_HTTP_CLIENT_CURL=ON, see install_otel.sh).
// When the macro is undefined everything compiles to no-op stubs.

#include "tracing.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "request_context.h"

#ifdef MOONCAKE_ENABLE_OTEL_TRACING
#include "opentelemetry/ext/http/client/http_client_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_options.h"
#include "opentelemetry/ext/http/client/http_client.h"
#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/nostd/span.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#include "opentelemetry/sdk/trace/batch_span_processor_options.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/processor.h"
#include "opentelemetry/sdk/trace/provider.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#include "opentelemetry/trace/provider.h"
#include "opentelemetry/trace/span.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/span_id.h"
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/trace/span_startoptions.h"
#include "opentelemetry/trace/trace_flags.h"
#include "opentelemetry/trace/trace_id.h"
#include "opentelemetry/trace/tracer.h"

namespace http_client = opentelemetry::ext::http::client;
namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace resource_sdk = opentelemetry::sdk::resource;
namespace otlp = opentelemetry::exporter::otlp;
namespace nostd = opentelemetry::nostd;
#endif  // MOONCAKE_ENABLE_OTEL_TRACING

namespace mooncake {

#ifdef MOONCAKE_ENABLE_OTEL_TRACING
namespace {

// ---------------------------------------------------------------------------
// Small hex <-> id helpers
// ---------------------------------------------------------------------------

template <std::size_t N>
std::optional<std::array<std::uint8_t, N>> HexToBytes(std::string_view hex) {
    if (hex.size() != 2 * N) return std::nullopt;
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    std::array<std::uint8_t, N> out{};
    for (std::size_t i = 0; i < N; ++i) {
        int hi = nibble(hex[2 * i]);
        int lo = nibble(hex[2 * i + 1]);
        if (hi < 0 || lo < 0) return std::nullopt;
        out[i] = static_cast<std::uint8_t>((hi << 4) | lo);
    }
    return out;
}

std::string TraceIdHex(const trace_api::TraceId& id) {
    char buf[trace_api::TraceId::kSize * 2];
    id.ToLowerBase16(
        nostd::span<char, trace_api::TraceId::kSize * 2>(buf, trace_api::TraceId::kSize * 2));
    return std::string(buf, trace_api::TraceId::kSize * 2);
}

std::string SpanIdHex(const trace_api::SpanId& id) {
    char buf[trace_api::SpanId::kSize * 2];
    id.ToLowerBase16(
        nostd::span<char, trace_api::SpanId::kSize * 2>(buf, trace_api::SpanId::kSize * 2));
    return std::string(buf, trace_api::SpanId::kSize * 2);
}

// Build a (possibly invalid) remote SpanContext from a propagated
// RequestContext. `parent_span_id_out` receives the incoming parent span id
// (ctx->span_id) so the new span can record it as its own parent_span_id.
trace_api::SpanContext MakeRemoteSpanContext(const RequestContext* ctx,
                                             std::string& parent_span_id_out) {
    parent_span_id_out.clear();
    if (ctx == nullptr) return trace_api::SpanContext::GetInvalid();
    parent_span_id_out = ctx->span_id;
    auto tid = HexToBytes<trace_api::TraceId::kSize>(ctx->trace_id);
    auto sid = HexToBytes<trace_api::SpanId::kSize>(ctx->span_id);
    if (!tid || !sid) return trace_api::SpanContext::GetInvalid();
    trace_api::TraceId trace_id(
        nostd::span<const std::uint8_t, trace_api::TraceId::kSize>(tid->data(),
                                                                   trace_api::TraceId::kSize));
    trace_api::SpanId span_id(
        nostd::span<const std::uint8_t, trace_api::SpanId::kSize>(sid->data(),
                                                                  trace_api::SpanId::kSize));
    return trace_api::SpanContext(
        trace_id, span_id,
        trace_api::TraceFlags(trace_api::TraceFlags::kIsSampled),
        /*is_remote=*/true);
}

// ---------------------------------------------------------------------------
// Global tracing state
// ---------------------------------------------------------------------------

std::atomic<bool> g_tracing_enabled{false};
std::shared_ptr<trace_sdk::TracerProvider> g_provider;

std::string NormalizeTracesEndpoint(const std::string& endpoint) {
    std::string url = endpoint;
    while (url.size() > 1 && url.back() == '/') url.pop_back();
    const auto scheme_end = url.find("://");
    const std::size_t host_start = (scheme_end == std::string::npos) ? 0 : scheme_end + 3;
    // No path present -> append the standard OTLP/HTTP traces path.
    if (url.find('/', host_start) == std::string::npos) url += "/v1/traces";
    return url;
}

}  // namespace

class ScopedSpanImpl {
   public:
    ScopedSpanImpl(const char* tracer_name, const char* span_name,
                   const RequestContext* parent_ctx) {
        auto provider = trace_api::Provider::GetTracerProvider();
        tracer_ = provider->GetTracer(tracer_name);
        trace_api::StartSpanOptions opts;
        opts.kind = trace_api::SpanKind::kServer;
        std::string parent_span_id;
        auto remote = MakeRemoteSpanContext(parent_ctx, parent_span_id);
        if (remote.IsValid()) opts.parent = remote;
        span_ = tracer_->StartSpan(span_name, opts);
        parent_span_hex_ = std::move(parent_span_id);
        // Record the application-level correlation id as a span attribute so the
        // trace can be cross-referenced with request-scoped logs. request_id is
        // carried unchanged across hops (PopulateRequestContext only refreshes
        // trace/span ids), so this stays stable for the whole request chain.
        if (parent_ctx != nullptr && !parent_ctx->request_id.empty()) {
            span_->SetAttribute(
                "request.id",
                nostd::string_view(parent_ctx->request_id.data(),
                                   parent_ctx->request_id.size()));
        }
    }
    ~ScopedSpanImpl() {
        if (span_) span_->End();
    }
    void PopulateRequestContext(RequestContext& ctx) const {
        if (!span_) return;
        auto sc = span_->GetContext();
        ctx.trace_id = TraceIdHex(sc.trace_id());
        ctx.span_id = SpanIdHex(sc.span_id());
        ctx.parent_span_id = parent_span_hex_;
    }

    // --- span enrichment (no-op when the span was never started) ---
    void SetAttribute(const char* key, std::string_view value) {
        if (span_) {
            span_->SetAttribute(key, nostd::string_view(value.data(), value.size()));
        }
    }
    void SetAttribute(const char* key, std::int64_t value) {
        if (span_) span_->SetAttribute(key, value);
    }
    void SetAttribute(const char* key, bool value) {
        if (span_) span_->SetAttribute(key, value);
    }
    void SetError(std::string_view description) {
        if (span_) {
            span_->SetStatus(
                trace_api::StatusCode::kError,
                nostd::string_view(description.data(), description.size()));
        }
    }
    void SetOk() {
        if (span_) span_->SetStatus(trace_api::StatusCode::kOk);
    }
    void AddEvent(std::string_view name) {
        if (span_)
            span_->AddEvent(nostd::string_view(name.data(), name.size()));
    }

   private:
    nostd::shared_ptr<trace_api::Tracer> tracer_;
    nostd::shared_ptr<trace_api::Span> span_;
    std::string parent_span_hex_;
};

bool InitTracing(const std::string& otlp_http_endpoint, std::string service_name) {
    if (otlp_http_endpoint.empty()) return false;
    if (g_tracing_enabled.load()) return true;

    otlp::OtlpHttpExporterOptions opts;
    opts.url = NormalizeTracesEndpoint(otlp_http_endpoint);
    opts.content_type = otlp::HttpRequestContentType::kBinary;
    opts.timeout = std::chrono::seconds(30);

    auto exporter = otlp::OtlpHttpExporterFactory::Create(opts);

    trace_sdk::BatchSpanProcessorOptions bsp_opts{};
    bsp_opts.max_queue_size = 2048;
    bsp_opts.schedule_delay_millis = std::chrono::milliseconds(5000);
    bsp_opts.max_export_batch_size = 512;

    auto processor = trace_sdk::BatchSpanProcessorFactory::Create(std::move(exporter), bsp_opts);

    resource_sdk::ResourceAttributes attr = {{"service.name", std::move(service_name)}};
    auto resource = resource_sdk::Resource::Create(attr);

    g_provider = std::shared_ptr<trace_sdk::TracerProvider>(
        trace_sdk::TracerProviderFactory::Create(std::move(processor), resource));
    std::shared_ptr<opentelemetry::trace::TracerProvider> api_provider = g_provider;
    trace_sdk::Provider::SetTracerProvider(api_provider);

    g_tracing_enabled.store(true);
    return true;
}

void ShutdownTracing() {
    g_tracing_enabled.store(false);
    if (g_provider) g_provider->ForceFlush();
    g_provider.reset();
    std::shared_ptr<opentelemetry::trace::TracerProvider> none;
    trace_sdk::Provider::SetTracerProvider(none);
}

bool IsTracingEnabled() { return g_tracing_enabled.load(); }

ScopedSpan::ScopedSpan(const char* tracer_name, const char* span_name,
                       const RequestContext* parent_ctx)
    : impl_(IsTracingEnabled() ? std::make_unique<ScopedSpanImpl>(tracer_name, span_name, parent_ctx)
                               : nullptr) {}
ScopedSpan::~ScopedSpan() = default;
bool ScopedSpan::active() const { return impl_ != nullptr; }
void ScopedSpan::PopulateRequestContext(RequestContext& ctx) const {
    if (impl_) impl_->PopulateRequestContext(ctx);
}
void ScopedSpan::AddAttribute(const char* key, std::string_view value) {
    if (impl_) impl_->SetAttribute(key, value);
}
void ScopedSpan::AddAttribute(const char* key, std::int64_t value) {
    if (impl_) impl_->SetAttribute(key, value);
}
void ScopedSpan::AddAttribute(const char* key, bool value) {
    if (impl_) impl_->SetAttribute(key, value);
}
void ScopedSpan::SetError(std::string_view description) {
    if (impl_) impl_->SetError(description);
}
void ScopedSpan::SetOk() {
    if (impl_) impl_->SetOk();
}
void ScopedSpan::AddEvent(std::string_view name) {
    if (impl_) impl_->AddEvent(name);
}

#else  // MOONCAKE_ENABLE_OTEL_TRACING disabled: empty stubs.

class ScopedSpanImpl {};

static std::atomic<bool> g_tracing_enabled{false};

bool InitTracing(const std::string& otlp_http_endpoint, std ::string /*service_name*/) {
    (void)otlp_http_endpoint;
    return false;
}
void ShutdownTracing() {}
bool IsTracingEnabled() { return false; }

ScopedSpan::ScopedSpan(const char* /*tracer_name*/, const char* /*span_name*/,
                       const RequestContext* /*parent_ctx*/)
    : impl_(nullptr) {}
ScopedSpan::~ScopedSpan() = default;
bool ScopedSpan::active() const { return false; }
void ScopedSpan::PopulateRequestContext(RequestContext& /*ctx*/) const {}
void ScopedSpan::AddAttribute(const char* /*key*/, std::string_view /*value*/) {}
void ScopedSpan::AddAttribute(const char* /*key*/, std::int64_t /*value*/) {}
void ScopedSpan::AddAttribute(const char* /*key*/, bool /*value*/) {}
void ScopedSpan::SetError(std::string_view /*description*/) {}
void ScopedSpan::SetOk() {}
void ScopedSpan::AddEvent(std::string_view /*name*/) {}

#endif  // MOONCAKE_ENABLE_OTEL_TRACING

}  // namespace mooncake
