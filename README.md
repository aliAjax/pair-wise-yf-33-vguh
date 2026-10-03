# 多站卫星地面站排程系统

使用标准库与 SQLite 实现的独立排程原型。系统维护卫星、地面站、天线、维护时段、可见窗口、租户配额和数据请求，并检查速率、数据量、截止时间、设备重叠、卫星同时接收、天气和租户配额。

## 接力接收

一个请求可以拆成多个**接力段**（`schedules` 即接力段，`request_id` 不再唯一，段内以 `segment_no` 编号），由多个地面站各承担一段：

- 已收数据与天线占用保留：窗口缩短或天气转坏时，正在接收的一段按实际可接收时长结算为 `received`（天线与占用时段保留为历史记录），未开始段置为 `preempted`；剩余未收数据自动重排到后续清晰窗口的空闲天线上。
- 窗口变化即重算受影响的未开始段：`POST /api/visibility-windows/{id}/change` 返回每段影响（`preserve_received_data` / `relay_handover` / `preempted` / `unchanged`）及重排后的 `relay_segments`。
- 天气转坏：`POST /api/stations/{id}/weather` 将该站受影响段重排到其他清晰窗口。
- 指挥官按优先级抢占排队段：`POST /api/schedules/{id}/preempt` 需提供 `priority` 或受益请求 `request_id`；被抢占段优先级不高于己方时返回 `priority_too_high`，抢占后未收数据同样接力到后续窗口。
- 并发写入采用乐观并发：提交可带 `if_match`（所依据的版次），过期返回 `revision_conflict` 并附 `latest_revision`；重叠时段先写入者成立，后到者返回 `antenna_conflict`/`satellite_conflict` 及最新版次。
- 租户只能查看自己请求的接力段；审计角色可查看但不能修改（任何写操作返回 `auditor_readonly`）。

## 运行

```bash
python3 app.py --db satellite_scheduling.db
```

默认监听 `127.0.0.1:8204`，首页 `/`，健康检查 `/health`。

身份头为 `X-User-Id`、`X-Role`；`requester` 还需 `X-Tenant`。角色：`viewer`、`requester`、`operator`、`commander`、`auditor`。

## 主要接口

- `POST /api/satellites`、`/api/stations`、`/api/antennas`、`/api/maintenance`、`/api/visibility-windows`、`/api/quotas`：资源配置。
- `POST /api/requests`：创建数据接收请求。
- `POST /api/requests/{id}/schedule`：排定单段接收。
- `POST /api/requests/{id}/relay`：接力排程。`segments` 显式给出各站接力段，或 `auto:true` 将剩余未收数据自动分配到后续可用窗口；可带 `if_match` 乐观版次。
- `POST /api/requests/{id}/reschedule`：重排被抢占请求。
- `POST /api/schedules/{id}/start`、`/complete`、`/cancel`、`/preempt`：接收状态、取消与指挥官紧急抢占（按优先级）。
- `POST /api/visibility-windows/{id}/change`：窗口变化并重算受影响接力段；已收数据保留。
- `POST /api/stations/{id}/weather`：天气转坏时重排该站受影响接力段。
- `GET /api/state`、`GET /api/schedules/{id}`：权限化状态查询（租户仅见自己的接力段）。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

速率和容量按静态 Mbps 与时长计算，不包含链路预算、调制编码、雨衰、天线跟踪和存储卸载策略。自动接力按时间顺序贪心分配后续清晰窗口，不做全局优化；租户身份使用请求头模拟；SQLite 和单进程 HTTP 服务适用于原型，生产环境需要统一身份、共享数据库和分布式资源锁。
