# 外部 API 通知服务

Go + gRPC + MySQL 实现的内部通知服务。业务系统提交标准事件，服务事务保存事件与各目标任务，后台 Worker 转换并调用外部 HTTP(S) API，保存结果并按幂等规则重试。

当前提供注册通知、CRM 状态变更和库存变更三个示例适配器，以及本地模拟供应商。真实供应商的字段映射、签名与成功规则，需要取得其协议后接入。

## 已实现功能

- Protobuf 定义的事件接入、事件/任务查询和管理 RPC；来源与管理凭据分别授权。
- MySQL 四张业务表、版本化迁移、来源事件唯一去重、内容冲突检查和事务扇出。
- 请求及配置快照、发送时读取凭据、Bearer/API Key/Basic/HMAC 认证。
- 固定执行池、单活 Worker、短事务领取任务、租约恢复和迟到响应留档。
- 单次超时、响应大小限制、成功响应契约、指数退避与抖动、次数和期限预算。
- 自然幂等、供应商幂等键及有效期检查；不确定结果不能安全重试时转为待核实。
- 管理重放、外部结果核实、操作去重与审计，gRPC 健康检查、TLS、Prometheus 指标和优雅退出。

范围不包含业务系统向本服务可靠发送、供应商隔离、限流、每供应商并发配额或暂停/恢复控制。下游按本次设计假定不会持续超时或被限流。固定执行池是全局任务池。

## 项目结构

```text
.
├── api/notification/v1/
│   ├── notification.proto             # 业务与管理 RPC 契约
│   ├── notification.pb.go             # 生成的消息类型
│   └── notification_grpc.pb.go        # 生成的客户端/服务端接口
├── cmd/
│   ├── notification-service/          # 通知服务启动入口与启动测试
│   └── mock-provider/                 # 广告、CRM、库存模拟供应商
├── internal/
│   ├── rpc/                           # RPC 实现、身份认证、权限与错误码
│   ├── app/                           # 事件去重、路由编排、重放和核实
│   ├── adapter/                       # 业务事件转换为供应商请求
│   ├── domain/                        # 领域模型、任务状态机与幂等规则
│   ├── repository/                    # MySQL 事务、领取、恢复和审计
│   ├── transport/                     # HTTP 请求、认证、超时与响应分类
│   ├── worker/                        # 轮询调度与全局固定执行池
│   ├── config/                        # 配置加载、默认值和校验
│   └── observability/                 # Prometheus 指标与 RPC 日志
├── migrations/
│   ├── 001_initial.sql                # 四张业务表的初始迁移
│   └── embed.go                       # 将迁移 SQL 嵌入服务程序
├── configs/
│   ├── example.json                   # 本机运行：供应商在 127.0.0.1:8081
│   └── docker.json                    # 容器运行：供应商在 mock-provider:8081
├── examples/                          # 注册、付款、购买 RPC 请求 JSON
├── tests/
│   ├── integration/                   # 真实 MySQL、RPC、Worker 集成测试
│   └── smoke/                         # 调用已启动服务的三场景冒烟程序
├── deploy/
│   ├── compose.yaml                   # MySQL、模拟供应商与通知服务编排
│   └── Dockerfile.prebuilt             # 使用本机编译产物构建容器
├── scripts/
│   ├── go.sh                          # Go 命令入口与仓库内缓存
│   ├── generate.sh                    # Protobuf 代码生成
│   └── build-local-image.sh            # 编译 Linux 程序并构建本地镜像
├── docs/                              # 技术方案、字段设计和执行进展
├── Dockerfile                         # 在线多阶段镜像构建
├── Makefile                           # 构建、测试、启动与停止命令
├── .env.example                       # 本地环境变量示例
├── go.mod / go.sum                    # Go 模块与锁定依赖
└── README.md                          # 项目入口说明
```

`*_test.go` 通常与被测代码放在同一目录。`bin/` 是构建输出，`.cache/` 是工具、依赖和编译缓存；这两个目录以及实际凭据文件 `.env` 已被 Git 忽略。

请求处理分为两段：

1. **接入阶段**：`rpc → app → config/adapter → repository`。认证并校验事件，匹配目标、生成请求快照，将事件和所有任务事务写入 MySQL，再返回接入成功。
2. **投递阶段**：`worker → repository → transport → 外部供应商 → repository`。领取到期任务，调用下游，保存尝试历史及任务状态；失败时按 `domain` 中的规则重试或转人工核实。

扩展供应商协议主要修改 `internal/adapter`，增加路由主要修改 `configs`；修改事件字段或 RPC 需先调整 `.proto` 并重新生成代码，不直接修改 `.pb.go` 文件。

设计见 [技术方案](docs/notification-service-v1-design.md)、[数据库字段](docs/notification-service-v1-database-design.md)；任务与验证记录见 [执行进展](docs/implementation-progress.md)。

一次实际运行的请求、下游接收与成功落库截图见 [完整请求验证](docs/full-request-verification.md)。

## 启动测试服务：Docker Compose

这里的“测试服务”是可以实际接收 RPC 的本地开发环境，包括通知服务、MySQL 和模拟供应商。`make test` 运行自动化测试，不会在后台留下测试服务。

### 1. 准备环境

启动 Docker Desktop，确保 Docker Engine 可用。使用本机镜像构建、冒烟程序或自动化测试时，还需要 Go 1.26 或以上。

所有命令从仓库根目录执行。当前电脑可以这样进入：

```sh
cd "/Users/east_melon/Documents/ChatGPT/right capital project"
# 已有 .env 时保留原配置。
test -f .env || cp .env.example .env
docker info > /dev/null
```

`.env.example` 包含本地 MySQL DSN 和四类 RPC 凭据；复制后可以直接用于示例环境。若已有数据卷，MySQL 密码须与首次初始化时一致，修改 `.env` 不会自动修改数据库内的密码。

### 2. 构建并启动

网络可访问 Docker Hub 时：

```sh
make up
docker compose --env-file .env -f deploy/compose.yaml up -d --wait --wait-timeout 90
```

第二条命令等待就绪，便于在启动完成后执行调用。

当前电脑此前访问 Docker Hub 出现过超时，已验证可使用下面的替代流程：

```sh
scripts/build-local-image.sh
docker compose --env-file .env -f deploy/compose.yaml up -d --no-build --wait --wait-timeout 90
```

替代流程使用本机 Go 编译 Linux 程序，从 `scratch` 构建镜像，不拉取 Go 基础镜像。仍需已缓存 MySQL 8.4 镜像或能够拉取它；Go 依赖也需已缓存或可下载。脚本自动匹配 Docker 的 amd64/arm64 架构，并复制本机 CA 文件；必要时可用 `CA_BUNDLE` 指定可信 PEM CA 文件路径。

### 3. 查看状态、日志与端口

```sh
docker compose --env-file .env -f deploy/compose.yaml ps
docker compose --env-file .env -f deploy/compose.yaml logs --tail 100 notification mock-provider
```

`mysql` 和 `notification` 应显示 `healthy`，`mock-provider` 应处于运行状态。服务启动时会自动执行数据库迁移并启动 Worker。

| 服务 | 本机访问地址 | 用途 |
| --- | --- | --- |
| 通知服务 | `127.0.0.1:50051` | 事件接入、查询、管理及 gRPC 健康检查 |
| Prometheus 指标 | `http://127.0.0.1:9090/metrics` | 查看运行指标 |
| MySQL | `127.0.0.1:33068` | 本地数据库连接，数据库名 notification |
| 模拟供应商 | Compose 内部 `mock-provider:8081` | 接收投递请求；未映射本机端口 |

Compose 环境开启开发明文 gRPC 和认证反射，凭据使用本地示例值。

### 4. 验证健康与完整投递

内置健康检查无需安装 grpcurl：

```sh
docker compose --env-file .env -f deploy/compose.yaml exec -T notification \
  /app/notification-service -mode healthcheck -insecure-dev
curl -fsS http://127.0.0.1:9090/metrics
```

健康检查无错误并以退出码 0 结束，表示数据库和 Worker 就绪。

加载本地凭据后运行冒烟测试：

```sh
set -a
. ./.env
set +a
make smoke
```

预期输出如下，任务 ID 以实际值为准：

```text
registration: accepted, deduplicated, delivered, history verified (task ...)
payment: accepted, deduplicated, delivered, history verified (task ...)
purchase: accepted, deduplicated, delivered, history verified (task ...)
```

冒烟测试通过本机 Go 调用已启动的 gRPC 服务，依次验证三类事件的接入、重复提交、最终成功状态和管理历史。重复运行使用相同事件 ID，不会重复创建任务；需要观察全新的投递时，另行提交新的事件 ID。`make smoke` 固定连接 `127.0.0.1:50051`，并使用 `examples/` 中的请求及 `.env` 中的凭据。

### 5. 停止服务

```sh
make down
```

停止服务后保留 MySQL 数据卷，下次启动可以继续使用原任务记录。初始迁移适用于全新数据库或相同迁移版本；之前实验性的五表数据库需要独立升级方案，不能直接套用本次初始迁移。

## 启动测试服务：本机 Go

这种方式适合断点调试 Go 代码。MySQL 仍由 Compose 提供，通知服务与模拟供应商作为本机进程运行。若之前启动了完整 Compose 环境，先执行 `make down`，避免本机端口及 Worker 单活锁冲突。

### 终端一：准备数据库，启动模拟供应商

```sh
cd "/Users/east_melon/Documents/ChatGPT/right capital project"
test -f .env || cp .env.example .env
make build
docker compose --env-file .env -f deploy/compose.yaml up -d --wait --wait-timeout 90 mysql
bin/mock-provider
```

模拟供应商监听 `127.0.0.1:8081`，该终端保持运行。

### 终端二：启动通知服务

```sh
cd "/Users/east_melon/Documents/ChatGPT/right capital project"
set -a
. ./.env
set +a
bin/notification-service -config configs/example.json -migrate -insecure-dev -reflection
```

该终端保持运行。`-migrate` 执行迁移，`-insecure-dev` 允许本地明文连接，`-reflection` 启用带认证的 RPC 反射。默认启动 API 和 Worker，gRPC 端口为 50051，指标端口为 9090。

### 终端三：检查与调用

```sh
cd "/Users/east_melon/Documents/ChatGPT/right capital project"
set -a
. ./.env
set +a
bin/notification-service -mode healthcheck -insecure-dev
make smoke
```

也可按下方“RPC 调试”章节使用 grpcurl。停止时在终端一、二分别按 `Ctrl+C`，再执行 `make down` 停止数据库。

服务停止接入后，Worker 最多等待 20 秒完成已领取请求，超时取消请求并限时保存结果；未完成持久化的执行由租约恢复处理。容器退出宽限期为 35 秒。

| 启动模式 | 用途 |
| --- | --- |
| `-mode all` | 默认模式，启动 API 与 Worker，适合本地测试 |
| `-mode api` | 只启动 RPC 与指标服务 |
| `-mode worker` | 只启动后台调度与指标服务 |
| `-mode migrate` | 执行迁移后退出 |
| `-mode healthcheck` | 检查目标 gRPC 服务就绪状态后退出 |

同一数据库只允许运行一个 Worker，第二个调度进程启动会失败。API 可以运行多个实例；同机分开启动 API/Worker 时，还须为各进程指定不同的 `-metrics-listen`。`-workers` 设置全局执行池，默认 8，范围 1–32。

## RPC 调试

本地可用 grpcurl 或 grpcui。开启反射时，反射同样需要凭据；不使用反射时可导入 `api/notification/v1/notification.proto`。所有业务和管理调用通过 metadata `authorization: Bearer ...` 授权。

```sh
grpcurl -plaintext -H "authorization: Bearer $BILLING_RPC_TOKEN" \
  127.0.0.1:50051 list

grpcurl -plaintext -H "authorization: Bearer $BILLING_RPC_TOKEN" \
  -d @ 127.0.0.1:50051 notification.v1.NotificationService/SubmitEvent \
  < examples/payment.json
```

注册和购买分别使用 `IDENTITY_RPC_TOKEN` / `SHOP_RPC_TOKEN` 和 `examples/registration.json` / `examples/purchase.json`。注册示例渠道为 `mock`。

返回 `accepted=true` 表示事件与任务已提交数据库；最终投递结果用响应里的任务 ID 查询。以下 ID 替换为实际响应中的值：

```sh
grpcurl -plaintext -H "authorization: Bearer $BILLING_RPC_TOKEN" \
  -d '{"id":"1"}' 127.0.0.1:50051 notification.v1.NotificationService/GetTask

grpcurl -plaintext -H "authorization: Bearer $ADMIN_RPC_TOKEN" \
  -d '{"limit":50}' 127.0.0.1:50051 notification.v1.ManagementService/ListTasks

grpcurl -plaintext -H "authorization: Bearer $ADMIN_RPC_TOKEN" \
  -d '{"taskId":"1"}' 127.0.0.1:50051 notification.v1.ManagementService/ListAttempts
```

重复提交同来源、同事件 ID、同内容返回原任务和 `duplicate=true`；更换内容但沿用事件 ID 返回 `ALREADY_EXISTS`。同一事件可能生成多个独立目标任务。

管理重放只允许 `failed` / `needs_review`，必须填写唯一 `operationId` 与原因。待核实或缺少安全幂等保障的任务，还须填写外部核实证据。相同操作 ID 和内容重复调用返回首次结果；内容改变返回冲突。结果核实通过 `ResolveTask`，`delivered=true` 标记成功，`false` 关闭失败记录，均需证据。

```sh
grpcurl -plaintext -H "authorization: Bearer $ADMIN_RPC_TOKEN" \
  -d '{"taskId":"1","operationId":"replay-001","reason":"供应商配置已修复","verificationEvidence":"供应商确认未执行"}' \
  127.0.0.1:50051 notification.v1.ManagementService/ReplayTask
```

重放保留原业务请求、幂等键、累计尝试数和历史，开启新轮次。供应商幂等键已过期时拒绝原任务重放，也不会自动续期或换键；须先核实、关闭旧任务，再按真实业务补偿方案处理。

## 配置与生产运行

`configs/example.json` 为本机示例，`configs/docker.json` 为 Compose 示例。配置文件保存路由、URL、适配器、超时、成功规则、重试预算与凭据**引用**。环境变量保存实际凭据，不写入数据库和日志。配置变更需重启；已创建任务继续使用其原请求与配置快照。凭据每次发送时读取，引用对应的环境值变更需要通过进程重启生效。

每个真实供应商必须明确成功状态及业务成功字段、错误语义、幂等支持与有效期。`natural` 只能用于真实协议保证重复调用不会新增副作用的接口；模拟供应商的能力不代表真实供应商能力。HMAC 示例签名为 `HMAC-SHA256(timestamp + "\n" + body)`，生产应按供应商规范扩展。

```sh
bin/notification-service -mode api -config configs/production.json \
  -listen 0.0.0.0:50051 -tls-cert /secrets/server.crt -tls-key /secrets/server.key
bin/notification-service -mode worker -config configs/production.json
```

生产默认要求 TLS，反射默认关闭，指标端口应仅对运维内网开放。来源凭据绑定来源，管理员凭据仅访问管理服务。标准 gRPC Health 的空服务名检查为免业务认证的就绪检查：API 检查数据库，合并模式同时检查 Worker。独立 Worker 通过运行日志及心跳指标监测。客户端健康检查可使用 `-mode healthcheck -listen 服务地址 -tls-ca CA文件`。

指标包括 `notification_delivery_attempts_total`、`notification_delivery_duration_seconds`、`notification_tasks`、`notification_oldest_due_seconds`、`notification_worker_heartbeat_timestamp_seconds`、`notification_rpc_requests_total`、`notification_storage_errors_total`。告警由外部 Prometheus/运维平台配置。

## 自动化测试与代码生成

| 命令 | 验证内容 | 是否依赖已启动测试服务 |
| --- | --- | --- |
| `make test` | 普通测试：事件契约、配置、状态机、适配、RPC 权限和 HTTP 行为 | 否；部分测试自行启动临时 HTTP 服务 |
| `make race` | 普通测试加 Go 竞态检测 | 否 |
| `make vet` | Go 静态检查 | 否 |
| `make build` | 构建两个本机启动程序到 bin/ | 否 |
| `make integration` | 全量测试加真实 MySQL 集成、TLS/启动/退出测试及竞态检测 | 只需 MySQL；测试自行启动临时 RPC/HTTP 服务 |
| `make smoke` | 调用已启动的通知服务，检查三类事件的完整流程 | 是；数据库、通知服务和模拟供应商均需可用 |

普通测试：

```sh
make test
make race
make vet
```

真实 MySQL 集成测试：

```sh
# 前置：准备 .env，启动 Docker Desktop。
docker compose --env-file .env -f deploy/compose.yaml up -d --wait --wait-timeout 90 mysql
set -a
. ./.env
set +a
make integration
```

集成测试显式使用 `integration` build tag，要求 `MYSQL_TEST_DSN`，缺失时失败而不跳过。仅允许回环地址上的 MySQL，账号须有创建/删除测试数据库权限；示例使用本地 root 账号。每次创建随机名称 `notification_it_*` 的独立数据库，结束只清理该数据库，不操作 DSN 指向的现有数据库。Go 竞态检测需支持 CGO 的本机编译器环境。

修改 `.proto` 后运行 `make generate`，需要预先安装 protoc 及标准 include 文件；脚本安装锁定版本的 Go 生成插件。普通构建使用已有生成代码，不要求 protoc。

`scripts/go.sh` 优先使用 `GO_BINARY` 指定路径、PATH 中的 Go 或仓库内可用工具链，将构建和依赖缓存放在被忽略的 `.cache` 中。如果本机未安装 Go，可以先安装 Go 1.26+，或将 `GO_BINARY` 设置为现有 Go 可执行文件的绝对路径。

## 常见启动问题

| 现象 | 检查与处理 |
| --- | --- |
| Docker daemon 无法连接 | 启动 Docker Desktop，确认 `docker info` 能成功执行 |
| 拉取 Go 基础镜像超时 | 使用 `scripts/build-local-image.sh` 及 `up --no-build` 的替代流程 |
| 报 `Go 1.26+ is required` | 安装 Go 或配置 `GO_BINARY`，然后重试构建/测试 |
| 端口占用 | 检查是否同时运行本机进程与完整 Compose 环境；停止不用的一组 |
| 数据库连接失败 | 等待 MySQL healthy，检查 `.env` 中的 DSN、账号和首次初始化密码 |
| RPC 凭据缺失或认证失败 | 在调用的终端重新加载 `.env`；服务端和客户端必须使用一致的凭据 |
| 第二个 Worker 启动失败 | 同库 Worker 单活；本机模式与 Compose 模式选择一组运行 |
| 迁移报校验和或已有表冲突 | 确认数据库属于当前四表版本；旧结构需单独迁移，不修改已执行的迁移文件 |
| 事件已接收但仍在等待 | 查看任务状态与 notification 日志，确认 Worker 运行且模拟供应商可达 |
