// Integration test: prove a per-request `request_id` set on the calling
// thread is carried through the coro_rpc out-of-band request attachment all the
// way into the master handler. The handler VLOG(1) emits the request_id, and
// this test intercepts that VLOG output via google::LogSink to turn the
// attachment round-trip into a deterministic CI assertion.
//
// Both single-key (invoke_rpc) and batch (invoke_batch_rpc) client templates
// snapshot current_request_context_attachment() at entry and send it via
// send_request_with_attachment. We exercise the single-key read route
// (GetReplicaList) and the batch-exist route (BatchExistKey) to prove the
// attachment bypass works for both the single and batch invocation templates.
//
// The seam: the V3 handlers VLOG(1) "HandlerName request_id=xxx trace_id=yyy".
// A custom LogSink captures these messages; the test extracts the request_id
// via regex. After the synchronous client call returns (syncAwait guarantees
// the handler already replied, hence happens-before), the sink has captured
// the log line. A nonexistent key still records the attachment before the
// lookup, so the key need not exist.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <regex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "centralized_master_client.h"
#include "default_config.h"
#include "master_config.h"
#include "request_context.h"
#include "rpc_types.h"
#include "test_server_helpers.h"
#include "types.h"

namespace mooncake {
namespace testing {
namespace {

// Custom LogSink that intercepts VLOG messages from the master handlers
// and extracts the request_id via regex.
class RequestIdLogSink : public google::LogSink {
   public:
    void send(google::LogSeverity severity, const char* /*full_filename*/,
              const char* /*base_filename*/, int /*line*/,
              const struct ::tm* /*tm_time*/, const char* message,
              size_t message_len) override {
        // VLOG maps to INFO severity; only capture those.
        if (severity != google::GLOG_INFO) return;
        std::string msg(message, message_len);
        std::smatch match;
        if (std::regex_search(msg, match, request_id_pattern_)) {
            last_request_id_ = match[1].str();
        }
    }

    // Returns the last captured request_id. Caller owns thread-safety
    // (single-threaded tests).
    std::string last_request_id_;

   private:
    std::regex request_id_pattern_{R"(request_id=(\S+))"};
};

class RequestIdAttachmentTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder{}.build()))
            << "failed to start in-process master";
        client_ = std::make_unique<CentralizedMasterClient>(generate_uuid());
        ASSERT_EQ(client_->Connect(master_.master_address()), ErrorCode::OK);

        // Enable VLOG(1) so handler log lines are emitted.
        FLAGS_v = 1;
        google::AddLogSink(&sink_);
        sink_.last_request_id_.clear();
    }

    void TearDown() override {
        google::RemoveLogSink(&sink_);
        clear_current_request_context();
        client_.reset();
        master_.Stop();
    }

    InProcMaster master_;
    std::unique_ptr<CentralizedMasterClient> client_;
    RequestIdLogSink sink_;
};

// A non-empty request_id set on the calling thread before a single-key read
// must appear in the handler's VLOG output, proving the attachment bypass works
// end to end.
TEST_F(RequestIdAttachmentTest, CarriesRequestIdOnReadRoute) {
    RequestContext ctx;
    ctx.request_id = "attach-123";
    set_current_request_context(std::move(ctx));

    auto result = client_->GetReplicaList("nonexistent_key_attachment_test");
    (void)result;  // OBJECT_NOT_FOUND is expected; the attachment is recorded
                   // before the lookup, which is all this test checks.

    EXPECT_EQ(sink_.last_request_id_, "attach-123")
        << "request_id was not propagated via the coro_rpc attachment";
}

// No per-request context on the calling thread => empty attachment => the
// handler VLOG block is skipped => no captured request_id.
TEST_F(RequestIdAttachmentTest, EmptyAttachmentWhenNoRequestId) {
    clear_current_request_context();

    auto result = client_->GetReplicaList("nonexistent_key_attachment_test");
    (void)result;

    EXPECT_TRUE(sink_.last_request_id_.empty())
        << "expected no observed request_id when none was set on the context";
}

// The batch-exist route goes through MasterClient::invoke_batch_rpc now using
// send_request_with_attachment; the BatchExistKey handler must log the id.
TEST_F(RequestIdAttachmentTest, CarriesRequestIdOnBatchExistRoute) {
    RequestContext ctx;
    ctx.request_id = "attach-batch-exist";
    set_current_request_context(std::move(ctx));

    std::string key = "nonexistent_key_batch_exist_attachment_test";
    std::vector<std::string_view> keys{key};
    auto result = client_->BatchExistKey(keys);
    (void)result;

    EXPECT_EQ(sink_.last_request_id_, "attach-batch-exist")
        << "request_id was not propagated via the coro_rpc attachment on the "
           "batch-exist route (invoke_batch_rpc)";
}

// Single-key exist route.
TEST_F(RequestIdAttachmentTest, CarriesRequestIdOnExistKeyRoute) {
    RequestContext ctx;
    ctx.request_id = "attach-exist-key";
    set_current_request_context(std::move(ctx));

    auto result = client_->ExistKey("nonexistent_key_exist_attachment_test");
    (void)result;

    EXPECT_EQ(sink_.last_request_id_, "attach-exist-key")
        << "request_id was not propagated via the coro_rpc attachment on the "
           "single-key exist route (invoke_rpc -> ExistKey)";
}

// Single-key remove route.
TEST_F(RequestIdAttachmentTest, CarriesRequestIdOnRemoveRoute) {
    RequestContext ctx;
    ctx.request_id = "attach-remove";
    set_current_request_context(std::move(ctx));

    auto result = client_->Remove("nonexistent_key_remove_attachment_test",
                                  /*force=*/false);
    (void)result;

    EXPECT_EQ(sink_.last_request_id_, "attach-remove")
        << "request_id was not propagated via the coro_rpc attachment on the "
           "single-key remove route (invoke_rpc -> Remove)";
}

// Single-key put-start route.
TEST_F(RequestIdAttachmentTest, CarriesRequestIdOnPutStartRoute) {
    RequestContext ctx;
    ctx.request_id = "attach-put-start";
    set_current_request_context(std::move(ctx));

    ReplicateConfig config;
    auto result = client_->PutStart("nonexistent_key_put_start_attachment_test",
                                    std::vector<size_t>{1024}, config);
    (void)result;

    EXPECT_EQ(sink_.last_request_id_, "attach-put-start")
        << "request_id was not propagated via the coro_rpc attachment on the "
           "single-key put-start route (invoke_rpc -> PutStart)";
}

// ---------------------------------------------------------------------------
// Async-path regression tests for the per-request attachment.
//
// AsyncGetReplicaList forwards an EXPLICIT std::string ctx_attachment through
// invoke_rpc_async -> invoke_rpc_async_with_pool, which must NOT read the
// thread_local g_current_ctx. The earlier bug read g_current_ctx at the async
// coroutine's entry; because that coroutine is resumed on a worker thread
// (the detached RunReadWithRetry via the coro executor) whose thread_local is
// not this request's, the id was dropped (empty) or, with a leftover scope on
// that thread, cross-contaminated with another request's id.
//
// We reproduce both failure modes by driving AsyncGetReplicaList from a fresh
// std::thread (a worker whose thread_local is independent of the test thread's)
// and asserting the handler VLOGs the EXPLICIT id, proving the attachment is
// carried by value rather than re-read from a worker thread's thread_local.
// ---------------------------------------------------------------------------

// Loss regression: a worker thread with no per-request context must still
// propagate the explicitly-snapshot attachment. A buggy version re-reading
// thread_local would send an empty attachment and the handler would not log
// the id (sink stays empty).
TEST_F(RequestIdAttachmentTest, AsyncCarriesExplicitAttachmentNoLoss) {
    RequestContext ctx;
    ctx.request_id = "async-explicit";
    set_current_request_context(std::move(ctx));
    std::string attachment = current_request_context_attachment();
    clear_current_request_context();  // The worker thread below never sets it.

    sink_.last_request_id_.clear();
    std::thread worker([&] {
        auto result = async_simple::coro::syncAwait(
            client_->AsyncGetReplicaList(
                "nonexistent_key_async_loss", GetReplicaListRequestConfig{},
                attachment));
        (void)result;
    });
    worker.join();

    EXPECT_EQ(sink_.last_request_id_, "async-explicit")
        << "AsyncGetReplicaList dropped the per-request id on the async/worker "
           "path (handler saw no attachment); invoke_rpc_async_with_pool must "
           "carry the explicit attachment, not read an empty thread_local";
}

// Pollution regression: a worker thread carrying a DIFFERENT request's context
// in its thread_local must NOT leak it into this request's master RPC. A buggy
// version re-reading thread_local would attach "async-intruder"; the fix must
// send the explicit "async-owner".
TEST_F(RequestIdAttachmentTest, AsyncUsesExplicitAttachmentNotWorkerThreadLocal) {
    RequestContext ctx;
    ctx.request_id = "async-owner";
    set_current_request_context(std::move(ctx));
    std::string owner_attachment = current_request_context_attachment();
    clear_current_request_context();

    sink_.last_request_id_.clear();
    std::thread worker([&] {
        // Leave a different request's context on this worker's thread_local to
        // mimic a live/residual CurrentCtxScope on a shared worker thread.
        RequestContext intruder;
        intruder.request_id = "async-intruder";
        set_current_request_context(std::move(intruder));

        auto result = async_simple::coro::syncAwait(
            client_->AsyncGetReplicaList(
                "nonexistent_key_async_pollute", GetReplicaListRequestConfig{},
                owner_attachment));
        (void)result;
        clear_current_request_context();
    });
    worker.join();

    EXPECT_EQ(sink_.last_request_id_, "async-owner")
        << "AsyncGetReplicaList cross-contaminated the request_id by reading "
           "the worker thread's thread_local instead of the explicit "
           "attachment";
    EXPECT_NE(sink_.last_request_id_, "async-intruder");
}

// Note: the dummy hop A->B bridge is exercised only end-to-end (DummyClient +
// real-client coro_rpc server + master). This in-process harness drives the
// real path (hop B only), so it does not cover the hop A bridge; that remains
// verified by reasoning (same-thread syncAwait + CurrentCtxScope, per plan).
// The async GetReplicaList path (explicit ctx_attachment carried through
// invoke_rpc_async / invoke_rpc_async_with_pool) is covered by the two tests
// above (no-loss + no-pollution across a non-request worker thread).

}  // namespace
}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    mooncake::init_ylt_log_level();
    return RUN_ALL_TESTS();
}
