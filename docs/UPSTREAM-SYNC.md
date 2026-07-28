# colab-vscode 上游跟踪

本文档跟踪 colab-cli 对 colab-vscode 上游实现的同步状态。每次审查后记录基准 commit、需要同步的变更、以及已完成的同步操作。

上游仓库本地路径：`/Users/lorne/dev26/colab-runtime-2/colab-vscode`

---

## 当前基准

| 项 | 值 |
|---|---|
| 基准 commit | `af9192a` build(deps-dev): bump linkify-it from 5.0.1 to 5.0.2 (#651) |
| 基准日期 | 2026-07-28 |
| 审查日期 | 2026-07-28 |
| 上游 HEAD | `af9192a`（与基准一致） |

> 基准已推进至上游 HEAD：截至本次审查的 commit 均已审阅并归类。其中「公共 API 迁移系列」是**已审阅但主动延后**的决策项，记录在下方「待同步变更」中，不因基准推进而丢失。下次审查从 `af9192a` 起 diff 即可。

---

## 待同步变更

### P0：Colab 公共 API（v2）迁移系列

上游正在把控制面从私有接口（`colab.research.google.com` TFE + `colab.pa.googleapis.com`）迁移到公共 API `https://colaboratory.googleapis.com`。这是自本文档建立以来影响最大的一次上游变更，涉及 11 个 commit：

| commit | 说明 |
|---|---|
| `3570773` feat: generate Colab OpenAPI schema (#623) | 引入 v1/v1beta OpenAPI JSON |
| `a266653` feat: new ColabApiClient with openapi-generator (#626) | openapi-generator 生成 `ColaboratoryApi` + `OperationsApi`，middleware 注入 auth / 错误处理 |
| `d77df24` chore: move `colab/client.ts` to `colab/client/v1` (#627) | 旧客户端挪到 `v1/`，与 v2 并存 |
| `eabab9f` feat: migrate `ColabJupyterServerProvider` to public API (#628) | `GetSubscription` 取代 `getUserInfo().subscriptionTier`；新增实验开关 `enable_public_api_vscode` |
| `8c50022` feat: migrate `getAvailableServerDescriptors` to public API (#630) | `ListRuntimeSpecs` 取代 `getUserInfo().eligibleAccelerators` + 手工拼 highmem |
| `67e416a` chore: `Subscription.tier` as required (#632) | tier 变必填 |
| `9d84329` refactor: lift error definitions to `colab/errors.ts` (#636) | 错误类型集中 |
| `d8952a6` chore: regen with `endpoint` and `ErrorInfo` (#638) | `ConnectionInfo.endpoint` 回填；错误详情结构化 |
| `3093eb0` feat: regen with `WaitOperation` (#640) | LRO 等待接口 `GET /v1/operations/{id}:wait?timeout=200s` |
| `32017d0` feat: migrate `assignServer` to `CreateRuntime` (#639) | 分配流程改为 `CreateRuntime` + `WaitOperation` LRO |
| `a52b48f` chore: regen with `Runtime.version` (#644) | `CreateRuntime` 支持指定 runtime 镜像版本 |

**公共 API 端点（`https://colaboratory.googleapis.com`）**

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/v1beta/runtimespecs` | 列出可用规格 + `eligible` 标志 |
| GET | `/v1beta/subscription` | 订阅等级 |
| GET | `/v1beta/runtimes` | 列出运行时 |
| POST | `/v1beta/runtimes?requestId=&runtimeId=` | 创建运行时（LRO） |
| GET/DELETE | `/v1beta/runtimes/{runtime}` | 查询 / 删除运行时 |
| GET | `/v1/operations/{id}:wait?timeout=200s` | 等待 LRO 完成 |

**已实测（2026-07-28，colab-cli 现有 OAuth 凭据）**

- `runtimespecs` / `subscription` / `runtimes` 三个 GET 均返回 200。
- 必须带 `X-Colab-Client-Agent: vscode`（colab-cli 的 `COLAB_CLIENT_AGENT_HEADER` 已有），否则 `403 PERMISSION_DENIED / reason: UNKNOWN_CLIENT_AGENT`。
- 现有 scope `https://www.googleapis.com/auth/colaboratory` 已足够，无需重新授权。
- 生产实验开关 `enable_public_api_vscode` 当前为 **false**——上游自身尚未放量，v1 路径仍是默认。

**决策：暂不迁移，等上游 `enable_public_api_vscode` 放量后再整体切换。**

不做部分迁移，也不保留 v1/v2 双路径回退——两套路径都要维护，且回退分支平时跑不到，真出问题时兜底的恰恰是没验证过的代码。要切就整体切。下方改造点仅作放量后的实施清单。

**放量后的改造清单**

1. `ListRuntimeSpecs`：直接返回 `{variant, accelerator, shape, eligible}` 组合，可替换 `runtime-manager.ts` 的 `resolveAccelerator()` / `resolveShape()`——后者目前靠 `isHighMemOnlyAccelerator()` 硬编码猜测 highmem 约束，而新 API 已把订阅等级和 highmem 规格算进去了。
2. `CreateRuntime` + `WaitOperation`：替换现有「GET 拿 xsrfToken → POST assign」两段式流程。错误判定从 HTTP 412/503 + `Outcome` 枚举，改为 `Operation.error.details[].reason`（`TOO_MANY_ACTIVE_RUNTIMES` / `DENYLISTED` / `QUOTA_EXCEEDED_USAGE_TIME`），加速器不可用则是 `code == FAILED_PRECONDITION && accelerator != 'NONE'`。
3. **ID 语义变更**：runtime ID 由后端分配（`runtimes/{id}`），不再是客户端 UUID。`runtimeId` 若自行指定须符合 RFC1034（1–63 字符、小写字母数字连字符、**必须字母开头**），因此 colab-cli 现有的 `randomUUID()` 不能直接复用。上游已把 `ColabJupyterServer.id`、`ServerStorage` 的类型从 `UUID` 放宽为 `string`；colab-cli 的 `StoredServer.id` 同理需放宽。
4. **token 过期语义变更**：`ConnectionInfo.expireTime` 是绝对时间戳，取代 v1 的 `tokenExpiresInSeconds` 相对秒数，影响 `token-refresher.ts` / `connection-refresher.ts`。
5. `ConnectionInfo.endpoint` 仍然回填，端口转发、隧道等基于 endpoint 的代码不受影响。
6. 公共 API **没有** runtime-proxy-token 刷新端点；刷新连接需改用 `GetRuntime` 重取 `connectionInfo`。上游此处尚未迁移，仍走 v1。

**复查触发条件**：下次审查先查 `https://colab.research.google.com/vscode/experiment-state` 的 `enable_public_api_vscode`。仍为 `false` 则本节整体顺延，无需重新分析。

### P3：`5ea91f4` fix: failing `upload file` E2E test (#622)

功能部分：`unassignServer` 前的会话清理抽成 `deleteSessions()`，用 `Promise.allSettled` + 吞异常改为尽力而为；列举 unowned 会话加 3s 超时。

colab-cli 的 `RuntimeManager.destroy()` 目前直接 `unassign`，不做会话清理。CLI 无 VS Code notebook 附着场景，收益有限，可选。

---

## 不影响 colab-cli 的变更

以下变更经审查确认无需同步（VS Code 特有功能、纯 build/CI/test、或 colab-cli 已有不同实现）：

| commit | 说明 | 跳过原因 |
|---|---|---|
| `b135a6c` refactor: client middleware (#505) | `ColabClient` 改为 middleware chain 架构 | colab-cli 用原生 fetch，架构不同 |
| `b13a5ce` feat: additional scopes to auth provider (#486) | incremental auth（`includeGrantedScopes`/`loginHint`） | CLI 一次性授权，暂不需要 |
| `e301bd1` fix: update drive scopes (#498) | Drive scope 改为 `drive.file` | colab-cli 用独立 OAuth client + `drive` 全权限 |
| `3ec9ea6` feat: add DriveClient (#503) | 裸 fetch 实现的 Drive client | colab-cli 已有 `googleapis` 包的实现 |
| `3735364` fix: unassign order (#525) | 先删 sessions 再 unassign | VS Code 特有状态管理 |
| `f24e86e` fix: revoke managed connections (#559) | `return` → `continue` 修复 | VS Code `JupyterConnectionManager` 特有 |
| `a54b347` fix: sort scopes (#517) | 排序 scopes | VS Code auth provider 特有 |
| `9e53188` fix: server not found event (#548) | provider 事件处理 | VS Code provider 特有 |
| `5e3de6a` fix: log process errors (#543) | extension 错误日志 | VS Code extension 特有 |
| `29c2542` feat: ConsumptionPoller (#530) | 消费轮询响应 assignment 变化 | VS Code UI 特有 |
| `b690ccc` feat: consumption status bar (#524) | 消费信息状态栏 | VS Code UI 特有 |
| `557d2e2` feat: enable terminal by default (#521) | package.json 开关 | VS Code 配置 |
| `92ddc04` feat: import notebook from URL (#463) | notebook 导入命令 | VS Code 命令 |
| `fbdfe57` feat: import deep-linking (#519) | URI handler 深度链接 | VS Code 特有 |
| `881d921` refactor: ExperimentStateProvider (#540) | 使用 SequentialTaskRunner | VS Code 特有 |
| `93d98e3` refactor: ResourceTreeProvider.getChildren (#523) | LatestCancelable | VS Code tree view |
| `cbb25b2` fix: guard disposed access (#514) | VS Code Disposable 生命周期 | VS Code 特有 |
| `9a075c1` refactor: resource error handling (#512) | 资源监控错误处理 | VS Code 特有 |
| `c24df57` / `f7bb8eb` / `3c0ddb5` / `1a2db3a` / `a5822a4` | ResourceTreeProvider 系列 | VS Code tree view |
| `8319ccc` refactor: OAuth timeout (#565) | e2e 测试超时 | 测试 |
| build/CI/deps/chore commits | 版本号、依赖升级、lint 配置等 | 无功能影响 |
| `e2227d0` fix: preserve Request headers in colabProxyFetch (#558) | 代理 fetch 保留调用方 header、proxy header 优先 | colab-cli 用 openapi middleware（`AddProxyToken`）已经 `.set()` 覆盖并保留原 header；该修复针对 node-fetch 的 `Request` 兼容，CLI 不涉及 |
| `312c085` fix: tolerate orphan assignment deletion races in getServers (#587) | 枚举外部 server 时对 listSessions 404 容错 | VS Code tree view 专用：CLI 的 `list()` 只调 `listAssignments()`，不逐个 `listSessions` 补 label |
| `d8c80db` fix: concurrent getChildren empty list (#588) | 并发 getChildren 返回空列表修复 | VS Code tree view 专用 |
| `50d6f31` refactor: split extension.ts into per-feature modules (#572) | activation 拆模块 | VS Code extension 专用 |
| `52c6395` feat: enrich assign_server_event (#577) | assign 流程加 outcome/configuration telemetry | VS Code telemetry；assign 错误处理逻辑本身未变，CLI 无 telemetry |
| `2a674b3` feat: telemetry for low/depleted CCU balance (#578) | CCU 余额通知 telemetry | VS Code telemetry |
| `e27c447` feat: telemetry for downloads (#576) | 下载 telemetry | VS Code telemetry |
| `26543dd` feat: telemetry for content browser file ops (#575) | 文件操作 telemetry | VS Code telemetry |
| `d0dbe15` feat: telemetry for opening terminal (#574) | 终端 telemetry | VS Code telemetry |
| `9717c1b` feat: telemetry for uploads (#573) | 上传 telemetry | VS Code telemetry |
| `a8bbf5e` feat: telemetry for notebook imports (#567) | notebook 导入 telemetry | VS Code telemetry |
| `e2280f9` / `efb8498` / `f07bfe8` / `e3dfe4d` test 系列 (#599/#581/#586/#583) | e2e/单测稳定性 | 测试 |
| `d5c7a9c` chore: upgrade actions node24 (#580) | CI runner 升级 | CI |
| `c45188e` chore: clean up generated Jupyter client (#629) | 生成物精简 + gitignore | colab-cli 不使用生成式 Jupyter 客户端 |
| `95c4ea3` / `e7826a5` OpenAPI JSON 排序 (#633/#634) | prettier `jsonRecursiveSort` | 格式化 |
| `e0923e8` fix: make Prettier checks work on Windows (#641) | 构建脚本 | 构建 |
| `fb86c39` test: update e2e test email selector (#613) | e2e 选择器 | 测试 |
| `57c92ef` chore: bump `google-auth-library` and `uuid` (#643) | 依赖升级 | 无功能影响 |
| `af9192a` / `9cda526` / `7996763` / `dfead29` / `279bfb1` / `254cdef` / `9a688d4` / `4896141` deps 系列 | linkify-it、fast-uri、js-yaml、markdown-it、ws、undici、form-data、esbuild 等 | 依赖升级 |

---

## 已完成的同步

### 2026-07-28 同步 `aa4d563` fix: `removeServer` to call Jupyter `sessions.list` instead of TFE (#646)

- 修改文件：`src/colab/client.ts`、`src/colab/api.ts`
- 删除 `ColabClient.listSessions()`（走 TFE 隧道 `tun/m/{endpoint}/api/sessions`，colab-cli 中无任何调用点）
- 删除随之悬空的 `SessionSchema` / `Session` 类型
- 保留 `TUN_ENDPOINT`（`assign`/`unassign`/`keep-alive`/凭据传播仍在用）与 `COLAB_TUNNEL_HEADER`（`sendKeepAlive` 仍在用）
- 保留 `KernelSchema`：删除 `SessionSchema` 后已无使用者，但上游同样保留，跟随上游以便后续比对
- 删除范围与上游 `aa4d563` 一致；未跟进"改为经 runtime proxy 直连 Jupyter"的部分，因为 CLI 无对应调用场景
- 备注：`colab runtime list` 走的是 `listAssignments()`，与本次删除无关，已实跑确认未受影响

### 2026-07-28 依赖安全修复（由上游 dependabot commit 触发的连带跟进）

上游 `279bfb1`(ws) / `6394d51`(qs) / `57c92ef`(uuid) 等 dependabot 提交本身无功能影响，但其指向的漏洞 colab-cli 因使用同批依赖而同样存在。经 `npm audit` 核实后处理：

- `ws` 8.19.0 → 8.21.1：修复 GHSA-58qx-3vcg-4xpx（未初始化内存泄露）、GHSA-96hv-2xvq-fx4p（小分片内存耗尽 DoS）。severity high，且 `ws` 位于 `terminal/terminal-connection.ts`、`jupyter/kernel-connection.ts` 核心路径（`colab exec` / `colab shell`）
- `qs` 6.15.0 → 6.15.3：修复 GHSA-q8mj-m7cp-5q26（`qs.stringify` DoS），传递依赖
- `uuid` 保持 10.0.0 **不升级**：漏洞 GHSA-w5hq-g745-h8pq 仅影响 v3/v5/v6 传 `buf` 参数的场景，colab-cli 三处导入（`auth-manager.ts`、`background-auth.ts`、`kernel-connection.ts`）**全部只用 v4 且不传 `buf`**，不受影响。修复需 `--force` 升至 uuid@14（breaking），代价大于收益。`gaxios` 的告警亦源于此，同样保留
- 仅 `package-lock.json` 变更，`package.json` 未动（均在既有 semver 范围内）
- 验证：`tsc --noEmit` 通过、`npm run build` 通过、`colab runtime list` 实跑正常。**`exec`/`shell` 的 WebSocket 路径未验证**（需活跃运行时），下次使用时留意

### 2026-04-20 同步 `98163e2` fix: amend free usage info API response (#547)

- 修改文件：`src/colab/api.ts`、`src/commands/usage.ts`
- `remainingTokens` 和 `nextRefillTimestampSec` 加 `.optional()`，schema transform 处理 undefined
- `usage.ts` 所有访问点加 nullish 保护

### 2026-04-20 同步 `6d599b3` feat: fallback to available accelerator(s) (#511)（仅 503 检测）

- 修改文件：`src/colab/client.ts`
- 新增 `AcceleratorUnavailableError` 错误类
- `assign()` 方法捕获 503 响应并抛出 `AcceleratorUnavailableError`
- 未实现加速器自动回退逻辑

### 2026-04-20 同步 `ae8ab3c` + `a5822a4` feat/refactor: resource monitoring API (#494, #504)

- 修改文件：`src/colab/api.ts`、`src/colab/client.ts`、`src/commands/runtime.ts`、`src/index.ts`
- 新增 `MemorySchema`、`GpuInfoSchema`、`FilesystemSchema`、`DiskSchema`、`ResourcesSchema` 及对应类型
- 新增 `getResources(proxyUrl, token)` 方法
- 新增 `colab runtime resources` 命令，展示 RAM / 磁盘 / GPU 使用情况

<!-- 模板：
### YYYY-MM-DD 同步 `<commit>` <title>

- 修改文件：...
- colab-cli commit：`<hash>`
- 备注：...
-->
