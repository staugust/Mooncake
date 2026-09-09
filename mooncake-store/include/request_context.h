#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <ylt/struct_pack.hpp>

namespace mooncake {

// Per-request context propagated through the store -> master path.
//
// This is independent of `client_id` (the stable lease/segment identity), which
// intentionally must NOT be reused as a per-request correlation id.
struct RequestContext {
    std::string request_id;  // application-level correlation id
    std::string trace_id;    // distributed trace id
    std::string span_id;
    std::string parent_span_id;
};

// Enable struct_pack field-name-based serialization. Future fields appended
// as struct_pack::compatible<std::string> at the end are safely ignored by
// older binaries that lack the field in their YLT_REFL list.
YLT_REFL(RequestContext, request_id, trace_id, span_id, parent_span_id);

// Per-thread current request context. Set on the calling (Python) thread before
// a store operation and consumed synchronously by the master-client wrappers on
// the same thread. For coroutine/async paths, snapshot it at entry and forward
// it explicitly instead of reading this in continuations.
inline thread_local std::optional<RequestContext> g_current_ctx;

// RAII scope that sets the current request context and restores the previous
// one on destruction (handy for the hop A->B bridge and for test/Python
// helpers).
class CurrentCtxScope {
   public:
    CurrentCtxScope() = default;
    explicit CurrentCtxScope(RequestContext ctx) : saved_(g_current_ctx) {
        g_current_ctx = std::move(ctx);
    }
    ~CurrentCtxScope() { g_current_ctx = std::move(saved_); }
    CurrentCtxScope(const CurrentCtxScope&) = delete;
    CurrentCtxScope& operator=(const CurrentCtxScope&) = delete;

   private:
    std::optional<RequestContext> saved_;
};

inline void set_current_request_context(RequestContext ctx) {
    g_current_ctx = std::move(ctx);
}
inline void clear_current_request_context() { g_current_ctx.reset(); }
inline const std::optional<RequestContext>& get_current_request_context() {
    return g_current_ctx;
}

// Bypass (out-of-band) attachment helpers. Client side: this is snapshotted at
// the entry of the master-client invoke_rpc* templates and handed to
// coro_rpc_client::send_request_with_attachment so request_id rides the request
// framing rather than a struct field. Server side: read it back via
// ctx.get_context_info()->release_request_attachment() (a std::string, which
// drains the buffer); an empty view means no per-request id was supplied.
// Serialize the full RequestContext to wire bytes for out-of-band
// attachment (coro_rpc send_request_with_attachment /
// release_request_attachment).
inline std::string current_request_context_attachment() {
    if (g_current_ctx) {
        return struct_pack::serialize<std::string>(*g_current_ctx);
    }
    return {};
}

// ---------------------------------------------------------------------------
// Trace id / span id derivation from a request id (header-only, no OTel dep).
//
// When an upstream caller supplies only a `request_id` but no trace context
// (the common case for UUID-tagged requests), the chain can still form a
// coherent trace by *using the request id as the trace id*. For a UUID request
// id the hyphens are simply stripped so the literal request id becomes the
// 32-hex trace id; for any other request id a deterministic FNV-1a hash is used
// so every hop seeds the same value. Pure, no OpenTelemetry dependency, and
// used both with tracing enabled (to seed the span's trace id) and disabled
// (to keep logs correlatable across hops).
// ---------------------------------------------------------------------------
inline char RequestContextLowHexChar(char c) {
    if (c >= 'A' && c <= 'F') return static_cast<char>(c - 'A' + 'a');
    return c;
}
inline bool RequestContextIsLowerHexChar(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
}
inline std::uint64_t RequestContextFnv1a64(std::string_view s, std::uint64_t seed) {
    std::uint64_t h = seed;
    for (unsigned char c : s) {
        h ^= static_cast<std::uint64_t>(c);
        h *= 0x100000001b3ULL;
    }
    return h;
}
inline std::string RequestContextBytesToHex(const unsigned char* data, std::size_t n) {
    static const char* kHex = "0123456789abcdef";
    std::string out;
    out.resize(n * 2);
    for (std::size_t i = 0; i < n; ++i) {
        out[2 * i] = kHex[(data[i] >> 4) & 0xf];
        out[2 * i + 1] = kHex[data[i] & 0xf];
    }
    return out;
}

// Derive a 32-hex trace id (16 bytes) from a request id. A hyphenated id that
// reduces to exactly 32 hex chars (a UUID, e.g. uuid4 minus its hyphens) is
// used verbatim so the trace id *is* the request id. Otherwise the request id
// is hashed deterministically; an all-zero id (invalid in OTel) is nudged.
inline std::string DeriveTraceIdFromRequestId(std::string_view request_id) {
    if (request_id.empty()) return {};
    std::string hex;
    hex.reserve(request_id.size());
    for (char c : request_id) {
        if (c == '-') continue;
        hex.push_back(RequestContextLowHexChar(c));
    }
    if (hex.size() == 32) {
        bool ok = true;
        for (char c : hex) {
            if (!RequestContextIsLowerHexChar(c)) {
                ok = false;
                break;
            }
        }
        if (ok) return hex;  // request id *is* the trace id (UUID case)
    }
    unsigned char bytes[16];
    std::uint64_t lo = RequestContextFnv1a64(request_id, 0xcbf29ce484222325ULL);
    std::uint64_t hi = RequestContextFnv1a64(request_id, 0x6c62272e07bb0142ULL);
    if (lo == 0 && hi == 0) lo = 1;  // all-zero TraceId is invalid
    for (int i = 0; i < 8; ++i) {
        bytes[i] = static_cast<unsigned char>(hi >> (56 - 8 * i));
        bytes[8 + i] = static_cast<unsigned char>(lo >> (56 - 8 * i));
    }
    return RequestContextBytesToHex(bytes, 16);
}

// When the upstream supplied only a request_id (no trace_id), use the request
// id as the trace id so the chain stays coherent even without an explicit
// trace context / OTel export. No-op when a trace id is already present.
inline void EnsureRequestIdAsTraceId(RequestContext& ctx) {
    if (!ctx.trace_id.empty() || ctx.request_id.empty()) return;
    ctx.trace_id = DeriveTraceIdFromRequestId(ctx.request_id);
}

// Deserialize a RequestContext from wire bytes (received via
// release_request_attachment). Returns an empty RequestContext when data is
// empty or deserialization fails.
inline RequestContext deserialize_request_context(std::string_view data) {
    RequestContext ctx;
    if (!data.empty()) {
        struct_pack::deserialize_to(ctx, data.data(), data.size());
    }
    // Self-seed the distributed trace id from the application request id when
    // the upstream caller provided only a request_id. This keeps the chain
    // coherent (and observable in logs) even when no explicit trace context /
    // OpenTelemetry export is configured. Idempotent: a context that already
    // carries a trace id (set by a previous hop) is left untouched, and the
    // derivation is deterministic, so every hop sees the same value.
    EnsureRequestIdAsTraceId(ctx);
    return ctx;
}

}  // namespace mooncake
