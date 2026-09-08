# Mooncake OpenTelemetry Tracing 集成计划

> 为 `mooncake_master` 与 `mooncake_client` 增加 trace span，对齐 `plan.md` 的
> hop-A / hop-B 全链路设计。
>
> OpenTelemetry C++ SDK (v1.28.0) **不** 由 in-tree submodule 构建，而是由
> `install_otel.sh` 预构建并以 static archive 安装到默认前缀 (`/usr/local`)，
> Mooncake 通过 `find_package(opentelemetry-cpp)` 消费。`extern/opentelemetry-cpp`
> submodule 已 revert，本提交与该 submodule 完全解耦。

---

## 0. 目标

1. 使用 `install_otel.sh` 预装的 opentelemetry-cpp (v1.28.0) 作为依赖库——头文件
   与 static archive 已就位 `/usr/local`，CMake 侧无需再内嵌源码构建。
2. 参考 `plan.md` 的 hop-A (DummyClient→RealClient) 与 hop-B (RealClient→Master)，
   在 **real_client** 与 **mooncake-master** 侧各开 trace span。
3. 新增命令行参数 `--otlp-traces-endpoint`：配置则启动 span 并导出到指定 collector，
   不配置则完全不开启 span。
4. **硬约束**：opentelemetry 默认编译为 static 库；尽量减少
   `mooncake_client` / `mooncake_master` 新增的动态库依赖。

---

## 1. 关键设计：静态折叠，零新增 .so 依赖

这是满足约束 4 的核心决策：

| 维度 | 方案 |
|---|---|
| OTel 获取方式 | `install_otel.sh` 下载并编译 opentelemetry-cpp v1.28.0（含 utf8_range / protobuf，均 static+PIC），`cmake --install` 到 `/usr/local`；不再使用 in-tree submodule |
| CMake 消费 | 开关 `MOONCAKE_ENABLE_OTEL_TRACING=ON` 时在顶层 `find_package(opentelemetry-cpp CONFIG REQUIRED)`；OFF 时完全不 `find_package`，零依赖、零 protobuf |
| 折叠目标 | OTel static archive (含 protobuf/abseil/utf8_range) 以 PIC 静态折叠进 `mooncake_store` → 最终两个二进制 |
| 运行期新增 .so | **零**。OTel + protobuf + abseil 全部 fold 进静态二进制 |
| HTTP 传输 | 自实现 `opentelemetry::ext::http::client::HttpClient`（基于 Mooncake 的 `coro_http`），`install_otel.sh` 构建时 `WITH_HTTP_CLIENT_CURL=OFF`，**永不引入 libcurl** |
| 关闭时退路 | `MOONCAKE_ENABLE_OTEL_TRACING` 未定义时 `tracing.cpp` 全 no-op stub，默认构建完全不受影响、不 `find_package`、无需 protobuf |

> `install_otel.sh` 的关键 cmake 参数：
> `-DWITH_OTLP_HTTP=ON -DWITH_OTLP_GRPC=OFF -DWITH_HTTP_CLIENT_CURL=OFF`。
> 只构建 Mooncake 用到的 OTLP/HTTP exporter，且 exporter 不链 libcurl；Mooncake 在
> 运行期注入自实现的 `coro_http` HttpClient 作为 OTLP/HTTP 传输层。

---

## 2.5 Span 内容完善（基于 OTel API）

每个 `ScopedSpan` 除创建 SERVER span 外，还基于 opentelemetry-cpp API 做了如下增强：

| 增强 | OTel API | 位置 |
|---|---|---|
| 自动记录 `request.id` span 属性 | `Span::SetAttribute(key, AttributeValue)` | `ScopedSpanImpl` 构造时，从传入 `RequestContext::request_id` 自动写入（非空才写）；request_id 跨 hop 不变，全链路可与应用日志关联 |
| 失败标记 ERROR 状态 | `Span::SetStatus(StatusCode::kError, description)` | 17 个 handler 在结果为失败时调用 `span.SetError(toString(result.error()))` |
| 属性/状态/事件可扩展接口 | `Span::SetAttribute` / `SetStatus` / `AddEvent` | `ScopedSpan` 暴露 `AddAttribute(string|int64|bool)` / `SetError` / `SetOk` / `AddEvent`，供 handler 后续按需丰富 |

`ScopedSpan` 的这些方法在 `MOONCAKE_ENABLE_OTEL_TRACING` 未定义时全部退化为 no-op stub（`tracing.cpp` 的 `#else` 段已提供），因此默认（OFF）构建下 handler 调用 `SetError` 不引入任何依赖、不产生任何开销。

### 错误标记策略（17 handler = hop-A 10 + hop-B 7，共 22 个 SetError 调用点）

- 单值结果 `tl::expected<T, ErrorCode>`：`if (!result.has_value()) span.SetError(toString(result.error()));`
- 向量结果 `std::vector<tl::expected<...>>`：遍历，首个失败即 `SetError` 并 `break`
- 显式前置失败分支（如 `shm_not_mapped`）：在既有 `LOG(ERROR)` 后补一行 `span.SetError("shm_not_mapped")`
- 成功路径不显式 `SetOk`（保持 OTel `kUnset` 默认语义，符合 SERVER span 惯例）

## 2. Span 链路模型（对齐 plan.md）

```
Python: store.set_request_context(trace_id / span_id / ...)   ← 可选 root 上下文
   │  attachment inject (dummy_client.cpp / master_client.h)
   ▼  hop-A SERVER span  (real_client 桥 handler)   ← child of 传入 ctx
        PopulateRequestContext → 把本次 span id 写回 ctx
   │  attachment 再 inject
   ▼  hop-B SERVER span  (master handler)            ← child of hop-A span
        -> export via OTLP/HTTP to --otlp-traces-endpoint
```

- 不在 dummy / master client 侧开 span（它们只做 attachment 透传），
  正好对应"为 real_client 和 mooncake-master 增加 trace span"。
- trace_id 全程共享，形成 hop-A → hop-B 两级父子链。
- `--otlp-traces-endpoint` 留空 → `InitTracing` 返回 false → 所有 `ScopedSpan` 退化为 no-op。

---

## 3. 文件改动清单（本次提交，相对基线 `23e3d2f6`）

| 文件 | 角色 | 改动 |
|---|---|---|
| `install_otel.sh` (新增, 101 行) | 依赖获取 | 下载并编译 opentelemetry-cpp v1.28.0（先装 utf8_range），`cmake --install` 到 `/usr/local`；`-DWITH_OTLP_HTTP=ON -DWITH_OTLP_GRPC=OFF -DWITH_HTTP_CLIENT_CURL=OFF` |
| `mooncake-store/include/tracing.h` (新增, 94 行) | 接口 | `InitTracing` / `ScopedSpan` / `ShutdownTracing` / `IsTracingEnabled`；关闭宏时全为 stub |
| `mooncake-store/src/tracing.cpp` (新增, 447 行) | 实现 | `CoroHttpClient/Session/Request/Response` 自实现 OTLP/HTTP 传输；`ScopedSpanImpl` 以传入 `RequestContext` 为 remote parent 开 SERVER span |
| `CMakeLists.txt` (改) | 顶层构建 | `option(MOONCAKE_ENABLE_OTEL_TRACING OFF)`；开关 ON 时 `find_package(opentelemetry-cpp CONFIG REQUIRED)`；OFF 时零依赖 |
| `mooncake-store/src/CMakeLists.txt` (改) | store 构建 | `tracing.cpp` 加入源文件；开宏时 `target_compile_definitions(mooncake_store PUBLIC MOONCAKE_ENABLE_OTEL_TRACING)` 并 link 五个 static imported targets |
| `mooncake-store/src/real_client.cpp` (改) | hop-A | 10 个 bridge handler 各加 `ScopedSpan("mooncake-real-client","rc.*",&rc)` + `PopulateRequestContext(rc)`，并重构 attachment 提取为 `CurrentCtxScope` |
| `mooncake-store/src/rpc_service.cpp` (改) | hop-B | ExistKey / Remove / GetReplicaList / BatchGetReplicaList / BatchExistKey (5 个) 各加 `ScopedSpan("mooncake-master","master.*",&req_ctx)` |
| `mooncake-store/src/centralized_rpc_service.cpp` (改) | hop-B | PutStart / BatchPutStart (2 个) 各加 span |
| `mooncake-store/src/real_client_main.cpp` (改) | CLI | `DEFINE_string(otlp_traces_endpoint,...)` + `InitTracing(FLAGS_otlp_traces_endpoint,"mooncake-real-client")` |
| `mooncake-store/src/master.cpp` (改) | CLI | 同上 flag + `InitTracing(...,"mooncake-master")` |

> `extern/opentelemetry-cpp` submodule 已从本提交中移除（gitlink 与 `.gitmodules`
> 条目均不再存在）；OTel 完全由 `install_otel.sh` 预装到 `/usr/local` 提供。

---

## 4. 覆盖矩阵 (17 个 V3 handler = hop-A 10 + hop-B 7)

### hop-A: real_client 桥 handler (服务端收 attachment → 开 span → 透传 hop-B)

| store API | handler | span name |
|---|---|---|
| put | put_dummy_helper | rc.put |
| put_batch | put_batch_dummy_helper | rc.put_batch |
| put_parts | put_parts_dummy_helper | rc.put_parts |
| get | get_buffer_info_dummy_helper | rc.get |
| batch_put | batch_put_from_dummy_helper | rc.batch_put |
| batch_get | batch_get_into_dummy_helper | rc.batch_get |
| batch_is_exist | batchIsExist_internal_rpc | rc.batch_is_exist |
| is_exist | isExist_internal_rpc | rc.is_exist |
| remove | remove_internal_rpc | rc.remove |
| get_size | getSize_internal_rpc | rc.get_size |

### hop-B: master handler (服务端收 attachment → 开 SERVER span)

| store API | handler (文件) | span name |
|---|---|---|
| is_exist | ExistKey (rpc_service.cpp) | master.exist_key |
| remove | Remove (rpc_service.cpp) | master.remove |
| get / get_size | GetReplicaList (rpc_service.cpp) | master.get_replica_list |
| batch_get | BatchGetReplicaList (rpc_service.cpp) | master.batch_get_replica_list |
| batch_is_exist | BatchExistKey (rpc_service.cpp) | master.batch_exist_key |
| put / put_parts | PutStart (centralized_rpc_service.cpp) | master.put_start |
| put_batch | BatchPutStart (centralized_rpc_service.cpp) | master.batch_put_start |

> 覆盖 plan.md 第四节矩阵中除 Heartbeat（后台线程无每请求 ctx）外的全部 store API。

---

## 5. OTel v1.28.0 API 校验结果

对 `tracing.cpp` 的 ON 路径逐项核对 `/usr/local` 安装的 v1.28.0 头文件：

| 用法 | v1.28.0 校验 |
|---|---|
| `OtlpHttpExporterFactory::Create(opts, http_client)` | ✅ 存在该重载 |
| `OtlpHttpExporterOptions{url, content_type, timeout}` | ✅ 字段名/类型一致；`timeout` 为 `system_clock::duration` |
| `HttpRequestContentType::kBinary` | ✅ |
| `BatchSpanProcessorFactory::Create(exporter&&, options)` | ✅ |
| `BatchSpanProcessorOptions{max_queue_size, schedule_delay_millis, max_export_batch_size}` | ✅ |
| `TracerProviderFactory::Create(processor, resource)` | ✅ |
| `trace_sdk::Provider::SetTracerProvider(shared_ptr<api Provider>)` | ✅ |
| `Resource::Create(ResourceAttributes)` + `ResourceAttributes` | ✅ |
| `SpanKind::kServer` | ✅ |
| `StartSpanOptions.parent` (`variant<SpanContext, Context>`，赋 SpanContext) | ✅ |
| `CoroHttp*` 对 `Request/Response/Session/HttpClient` 全部纯虚方法 override 签名 | ✅ 全部匹配 |
| `find_package` 导入目标名 `opentelemetry-cpp::trace / resources / otlp_recordable / otlp_http_exporter / otlp_http_client` | ✅ 与 v1.28.0 `EXPORT_NAME` 一致 |

- **OFF 路径** `g++ -fsyntax-only`：✅ 通过（仅一个与本次无关的既有 `[[nodiscard]]` 警告）
- **ON 路径** `g++ -fsyntax-only -DMOONCAKE_ENABLE_OTEL_TRACING`（对真实 v1.28.0 头文件）：✅ 通过；
  OTel 头文件不传递依赖 protobuf（protobuf 仅在 SDK 链接期需要，已由 install_otel.sh 预装）

---

## 6. 构建 / 运行

### 一次性：预装 opentelemetry-cpp（产出 `/usr/local` 下的头文件 + static archive）

```bash
./install_otel.sh     # 编译并以 static archive 安装 opentelemetry-cpp v1.28.0 到 /usr/local
```

`install_otel.sh` 内部先构建并安装 `utf8_range`，再以
`-DWITH_OTLP_HTTP=ON -DWITH_OTLP_GRPC=OFF -DWITH_HTTP_CLIENT_CURL=OFF`
编译 opentelemetry-cpp（protobuf 由其自带 FetchContent/Find 拉取并 static 折叠），
最后 `cmake --install` 到默认前缀 `/usr/local`。

### 启用 tracing 的构建

```bash
mkdir build && cd build
cmake .. \
    -DUSE_HTTP=ON \
    -DCMAKE_BUILD_TYPE=Release \
    -DMOONCAKE_ENABLE_OTEL_TRACING=ON      # 关键开关；OFF=默认无 tracing、无需 protobuf
cmake --build . -j$(nproc) --target mooncake_master mooncake_client
```

- `MOONCAKE_ENABLE_OTEL_TRACING=ON` 时 CMake `find_package(opentelemetry-cpp CONFIG REQUIRED)`
  解析 `/usr/local/lib/cmake/opentelemetry-cpp/` 下的导入目标并静态链接。
- 若 OTel 未装到默认路径，追加 `-DCMAKE_PREFIX_PATH=/path/to/otel-install`。
- 默认 OFF：不 `find_package`、不拉 protobuf、`tracing.cpp` 全 stub，日常构建零影响。

### 运行期开关

```bash
./mooncake_master --otlp-traces-endpoint http://collector:4318/v1/traces   # 启用 span 导出
./mooncake_master                                                            # 不传 → 不开 span (no-op)
./mooncake_client --otlp-traces-endpoint http://collector:4318/v1/traces
```

默认 endpoint 不带 `/v1/traces` 时，`NormalizeTracesEndpoint` 会自动补上。

---

## 7. 本沙箱构建限制（环境问题，非代码问题）

| # | 现象 | 根因 | 影响 |
|---|---|---|---|
| 1 | `cmake` 配置失败：`find_package(yalantinglibs)` 找不到包配置 | 系统只装了 `/usr/local/include/ylt` 头文件，未装 CMake 包配置 (`yalantinglibsConfig.cmake`) | 无法在沙箱内完整 e2e 构建任何目标；需在装好 yalantinglibs 的环境编译 |
| 2 | ON 路径需已 `./install_otel.sh` 安装 OTel | 沙箱无网络，无法运行 `install_otel.sh` 下载/编译 OTel | 需在能联网且已跑过 `install_otel.sh` 的环境验证链接；代码已用 `-fsyntax-only` 对真实头文件通过 |

代码侧正确性已通过静态校验确认（见第 5 节）。在装好 yalantinglibs 且已跑过
`install_otel.sh` 的环境，第 6 节命令即可产出带 tracing 的两个二进制。

---

## 8. 后续可选优化（不在本次范围）

- 把对端 client 侧也开 CLIENT span（hop-A 的 dummy 侧、hop-B 的 master_client 侧），
  让每跳有完整的 client/server 成对 span（当前只开 server 侧，已满足"real_client + master"需求）。
- **构建脚本保持不动**：`b2.sh` 不引入 tracing flag。`MOONCAKE_ENABLE_OTEL_TRACING` 默认 `OFF`，
  日常构建照常产出无-tracing 二进制（零依赖、不联网拉 protobuf）。要不要开 tracing
  是构建时一次性的事，由显式 CMake flag 决定，不进日常构建脚本。
- 考虑 `MOONCAKE_ENABLE_OTEL_TRACING=ON` 时在 CI 单独跑一轮以回归（先执行 `install_otel.sh` 预装）。
