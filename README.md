# 跨境交付联络权限与履约回执

按委托和用途披露联络信息，处理改派时的凭证与对端收敛。

本仓库在交换资料与本地对端录制之上，提供了一个**仅用 Python 标准库**实现的交付服务：

- 出口方报送交付单与收车方联络选择；区域运营方登记配送方、服务区域、获准字段；
- 调度在同一订单上发出、交接、结束配送委托，末端可再委托经登记的协力方；
- 个人资料按「订单当前同意字段 ∩ 配送方登记范围 ∩ 委托链范围」即时取数，委托只能相同或更小，且不越过订单用途与有效期；
- `proxy_only` 地区只发项目转接引用 `relay`，不发电话号码原值；
- 同一联络人在不同订单上的同意互不打通（按订单授权）；
- 披露走合作端 `POST /disclosures` 契约；停用只在合作端 `GET ...` 回 `stopped`（可带证据）后才算完成，断连/`stop_pending` 一律保持待收敛并在重启后续跑对账；
- 流水与披露记录只存**字段名**，不存个人资料原值；监管凭证保留，凭证有效性按签收时间对照委托区间判定；
- 运营授权视图回答：哪份凭证有效、哪些旧引用仍在合作方处理、哪笔需要人工跟进。

原始合成资料说明见 [交换资料约定.md](交换资料约定.md)。

## 运行环境与自检

使用 Python 3.11.2，无第三方依赖。

```sh
# 原有资料格式/引用检查 + 全部交付规则、对端收敛、HTTP 场景测试
python3 -m unittest discover -s tests -v

# 一键端到端联调剧本（自动起临时报文重放对端，22 项断言）
python3 demo_e2e.py

# 或一键执行上面两项
sh verify.sh
```

## 作为 Python 服务使用

核心实现在 [`service.py`](service.py)（`delivery_service.py` 是同名兼容入口）。
构造参数都可省略；给一个状态文件路径即可在重启后恢复：

```python
from service import DeliveryService

svc = DeliveryService("runtime/state.json",
                      peer_url="http://127.0.0.1:8087",
                      now="2026-11-27T03:00:00Z")   # now 也可传时钟函数

svc.allowed_fields("X3", "B1", "G-3", at="2026-11-27T01:30:00Z")
# -> {"phone", "drop_zone"}
svc.view_contact("X3", "G-3", "B1", ["phone"], at="2026-11-27T01:45:00Z")
# -> {"phone": "+1-202-555-0103"}     # 仅返回本次需要且获准的字段

svc.disclose("D-3", order="X3", grant="G-3", partner="B1", fields=["phone"])
svc.request_stop("D-3")               # 对端断连 -> 仍是 stop_pending，绝不显示完成
svc.reconcile()                      # 反复调用直到合作端回 stopped + evidence
svc.ops_overview(actor={"id": "ops", "roles": ["ops"]})
```

常用方法：`report_delivery`、`register_partner`、`upsert_contact`、
`record_dispatch/assign/delegate/end_grant`、`update_choice/change_receiver`、
`current_delegation`、`allowed_fields/scope_report/check_access`、
`view_contact/take_number`、`disclose/request_stop/reconcile`、
`advance_task`、`submit_proof/proof_validity/review_proofs`、
`ops_overview/followups/pending_convergence`、`audit_entries`。

## HTTP 服务

```sh
python3 app.py --port 8080 --state runtime/state.json \
  --peer-url http://127.0.0.1:8087 --reconcile-interval 5
# 也可用 python3 main.py / python3 server.py；端口读 DELIVERY_PORT/PORT
```

不带身份头的本机调用视为受信上下文；带 `X-Actor-Id`、`X-Actor-Role: ops`
时按角色授权。主要路由：

| 方法/路径 | 说明 |
| --- | --- |
| `GET /health` | 健康检查 |
| `GET /` | 极简运营看板（无外部依赖） |
| `POST /v1/deliveries` | 出口方报送交付单/选择 |
| `POST /v1/partners` | 运营登记配送方/区域/范围 |
| `POST /v1/contacts` | 登记合成联络资料 |
| `POST /v1/dispatch`、`POST /v1/grants` | assign / delegate / end |
| `GET /v1/orders/{order}` | 当前交付单 |
| `POST /v1/orders/{order}/choice` | 收货方改同意字段 |
| `POST /v1/orders/{order}/receiver` | 收货方换人（contact_ref） |
| `GET /v1/orders/{order}/delegation?at=` | 查当前委托链与有效字段 |
| `GET/POST /v1/contact`、`/v1/contact/view` | 按当前范围取资料（裸字段对象） |
| `GET/POST /v1/number` | 取号：普通地区 phone，转接地区 relay |
| `GET/POST /v1/scope` | 只算字段范围，不取值 |
| `POST /v1/tasks/event`、`GET /v1/orders/{o}/task` | 推进/查询交付任务（仅当前有效委托） |
| `POST /v1/disclosures` | 计算范围并调合作端 `/disclosures` |
| `GET /v1/disclosures[?status=&order=]` | 披露引用列表 |
| `GET /v1/disclosures/{ref}` | 披露/收敛状态 |
| `POST /v1/disclosures/{ref}/stop` | 请求合作端停用 |
| `POST /v1/reconcile[?ref=]` | 立即跑一轮对账 |
| `GET /v1/proofs`、`POST /v1/proofs`、`GET /v1/proofs/{receipt}` | 凭证补交/列表/有效性 |
| `GET /v1/ops/overview?live=1` | 运营授权总览 |
| `GET /v1/ops/followups` | 人工跟进队列 |
| `GET /v1/audit`、`/v1/flow` | 审计流水（无原值） |
| `POST /rpc` | `{"op":"方法名","args":{...}}` 统一调用面 |

### 与本地对端做一次真实联调

```sh
# 终端 A：合作端录制（默认 127.0.0.1:8087，重放 samples/peer.json）
python3 peer_replay.py --port 8087

# 终端 B：交付服务（剧本演练时可关掉后台自动对账，改为手动驱动）
python3 app.py --port 8088 --state runtime/live.json \
  --peer-url http://127.0.0.1:8087 --reconcile-interval 0

curl -s -X POST localhost:8088/v1/disclosures -H 'Content-Type: application/json' -d '{
  "ref":"D-3","order":"X3","grant":"G-3","partner":"B1",
  "fields":["phone"],"at":"2026-11-27T01:45:00Z"}'
# -> status active, received_fields ["phone"]；对端精确匹配 D-3/G-3/该号码

curl -s -X POST localhost:8088/v1/disclosures/D-3/stop -d '{}' \
  -H 'Content-Type: application/json'
# 录制对端直接断连 -> status=stop_pending, last_error=stop_no_response

# Ctrl-C 后用同一 --state 重新启动 app.py（模拟重启），再两次：
curl -s -X POST localhost:8088/v1/reconcile
# 第 1 次：合作端 stop_pending，仍未完成
curl -s -X POST localhost:8088/v1/reconcile
# 第 2 次：stopped + evidence S4
```

## 样例数据所确定的授权矩阵

时间锚点：M-1 `00:30` 派 G-3/B1；M-2 `01:00` 转委托 G-3A/B1a；
P3 `01:50` 签收；M-3/M-4 `02:00` 结束 G-3、改派 G-4/B2；P4 `02:09` 签收。

| 询问 | 结果 |
| --- | --- |
| X3 / B1 / G-3（02:00 前） | `phone` + `drop_zone`（值 `+1-202-555-0103`、`E-row7`） |
| X3 / B1a / G-3A | 仅 `drop_zone`（协力方范围不大于 B1） |
| X3 / B2 / G-4（02:00 后） | 仅 `drop_zone`（B2 未登记 phone） |
| X4（与 X3 同 R7/C-3，但 choice 只有 drop_zone） | 任何方式都取不到 phone |
| X5 / B3（R-Relay，proxy_only） | 仅 `relay-C8`，不返回 `+1-202-555-0108` |
| 02:00 后用 G-3/G-3A 查资料或推进任务 | 拒绝（`grant_inactive`） |
| 旧 B1 补交 01:55 签收的凭证 | 受理且有效，但不改变 G-4 当前任务 |
| P3 / P4 | P3 有效（签收在委托区间内，上传晚不影响）；P4 `after_end`，进人工跟进 |

## 持久化与隐私边界

- `--state` 指向的 JSON 原子落盘（临时文件 + `os.replace`），保存委托、披露状态、
  任务状态、跟进项与审计；重启后新建同路径实例即可继续对账。
- 个人资料原值只从 `samples/contacts.jsonl` 或独立的
  `contacts.registry.json`（运行时新增联络人，与状态同目录）解析；
  **state / 审计流水 / 披露单只保存 `contact_ref` 与字段名**，
  因此可以把流水给运营/监管看而不泄露原值。
- 合作端契约（见《交换资料约定》）：`POST /disclosures` 收发 `ref/grant/fields`，
  回执只含收到的字段名；`POST /disclosures/{ref}/stop` 收空对象；
  `GET /disclosures/{ref}` 返回 `active | stop_pending | stopped`，
  只有 `stopped`（可附 `evidence`）证明合作端已停用。状态码 `0` 表示断连无应答。
- 后台对账线程按 `--reconcile-interval`（默认 5 秒）运行并在启动时先对账一次；
  脚本/联调可用 `--reconcile-interval 0` 关闭后台节奏，改为显式 `POST /v1/reconcile`。

## 本地对端

运行 `python3 peer_replay.py` 可在 `127.0.0.1:8087` 重放 `samples/peer.json` 中的 HTTP 录制。`--recording` 可读取另一份同格式录制，`--port` 可更改端口。每条录制依次写请求方法、路径、JSON 请求体和响应序列。没有请求体时写 null。每种请求独立推进响应，最后一项可重复读取，状态码 0 代表连接断开且未收到应答。重启重放进程会重新开始读取录制，不代表现实中的远端状态被重置。
