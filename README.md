# 多站卫星地面站接力排程系统

使用标准库与 SQLite 实现的独立排程原型。系统维护卫星、地面站、天线、维护时段、可见窗口、租户配额和数据请求，并检查速率、数据量、截止时间、设备重叠、卫星同时接收、天气和租户配额。

核心能力：一个数据请求可以拆成多个**接力段（segment）**，由多个地面站/天线在各自可见窗口内分段接收；某站过站缩短或天气转坏时，未收完的数据自动转到后续可用窗口（可跨站），已收数据与天线占用保留。

## 运行

```bash
python3 app.py --db satellite_scheduling.db
```

默认监听 `127.0.0.1:8204`，首页 `/`，健康检查 `/health`。

身份头为 `X-User-Id`、`X-Role`；`requester` 还需 `X-Tenant`。角色：`viewer`、`requester`、`operator`、`commander`、`auditor`（审计角色全量只读）。

## 接力排程规则

- `POST /api/requests/{id}/schedule` 支持单段 JSON，或 `{"segments":[...]}` 一次提交多站接力段；同一卫星的各段时间不得重叠。
- 每段记录 `seq`、`planned_mb`（本段计划收货量）；完成接收时可传 `received_mb` 结算实收货量。
- 全部段计划容量必须覆盖请求剩余数据量，否则返回 `insufficient_capacity`。
- 数据收齐后其余排队段自动 `canceled`（原因 `data_complete`）；未收完则由后续段继续接力。
- `received` 段永久保留且继续占用天线与卫星时段，任何新排程与之重叠都返回 409。

## 主要接口

- `POST /api/satellites`、`/api/stations`、`/api/antennas`、`/api/maintenance`、`/api/visibility-windows`、`/api/quotas`：资源配置。
- `POST /api/requests`：创建数据接收请求。
- `POST /api/requests/{id}/schedule`、`/reschedule`：排程（支持多段）或重排被抢占请求。
- `POST /api/schedules/{id}/start`、`/complete`、`/cancel`、`/preempt`：接收状态与紧急抢占；`/complete` 可传 `received_mb`。
- `POST /api/visibility-windows/{id}/change`：窗口变化（revision +1），仅失效受影响的**未开始**段并在后续窗口重算；已收/接收中段保留。
- `POST /api/stations/{id}/weather`：天气转坏；该站排队段失效重算（跳过坏站窗口），接收中/已收保留。
- `POST /api/commands/priority-preempt`：仅指挥官；插入紧急段，按优先级挤掉更低优先级的**排队段**（接收中/已收受保护），被挤段自动在后续窗口接力；同级或更高优先级返回 `priority_conflict`。
- `GET /api/state`、`GET /api/schedules/{id}`、`GET /api/audit-log`：权限化状态查询（租户只见自己的请求与段，auditor 与排程角色可查审计日志）。

## 并发与版次

- 每个工作线程使用独立 SQLite 连接，所有写操作在 `BEGIN IMMEDIATE` 事务内完成（WAL + busy_timeout）。两个排程员并发提交重叠时段时严格串行：先写入者成立，后到者收到 409 `antenna_conflict`/`satellite_conflict`，并在 `details` 中拿到占用段的最新 `revision`、优先级和更新时间。
- 排程段可带 `window_revision`；若提交依据的窗口已被他人改过，返回 409 `stale_window_revision` 与 `current_revision`。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

速率和容量按静态 Mbps 与时长计算，不包含链路预算、调制编码、雨衰、天线跟踪和存储卸载策略。租户身份使用请求头模拟；SQLite 和单进程 HTTP 服务适用于原型，生产环境需要统一身份、共享数据库和分布式资源锁。
