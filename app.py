"""交付服务的 HTTP/JSON 入口（仅标准库）。

启动：
    python3 app.py --port 8080 --state runtime/state.json \
        --peer-url http://127.0.0.1:8087

设计要点：
* 所有业务规则都在 :class:`service.DeliveryService`，HTTP 只做编解码；
* 角色由网关注入的 ``X-Actor-Id`` / ``X-Actor-Role`` 头表达；不带身份头的
  回环调用视为本机受信上下文（system），带了身份就按角色授权；
* 后台线程周期性 reconcile，重启后自动继续对账；
* 另提供 ``POST /rpc`` 统一方法调用面，便于脚本/联调。
"""

from __future__ import annotations

import argparse
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from service import (
    DEFAULT_PEER_URL,
    DeliveryService,
    ServiceError,
    format_time,
)

ROOT = Path(__file__).resolve().parent
DEFAULT_STATE = ROOT / "runtime" / "state.json"

# /rpc 允许直接调用的方法白名单
RPC_OPS = {
    "load_feeds",
    "report_delivery",
    "register_partner",
    "upsert_contact",
    "update_choice",
    "change_receiver",
    "record_dispatch",
    "assign",
    "end_grant",
    "delegate",
    "current_delegation",
    "current_grant",
    "get_current_delegation",
    "allowed_fields",
    "effective_fields",
    "permitted_fields",
    "scope_report",
    "check_access",
    "can_access",
    "view_contact",
    "query_contact",
    "take_number",
    "advance_task",
    "advance_delivery",
    "task_status",
    "task_events",
    "disclose",
    "request_stop",
    "reconcile",
    "submit_proof",
    "proof_validity",
    "verify_proof",
    "review_proofs",
    "valid_proofs",
    "valid_proof",
    "dispatch_proof",
    "ops_overview",
    "followups",
    "manual_followups",
    "pending_convergence",
    "reference_status",
    "audit_entries",
    "get_delivery",
    "get_disclosure",
    "list_disclosures",
    "add_operator",
    "resolve_followup",
}


def create_service(state_path: "str | Path | None" = DEFAULT_STATE, *,
                   peer_url: str | None = DEFAULT_PEER_URL,
                   samples_dir: "str | Path | None" = None,
                   now=None, **kwargs) -> DeliveryService:
    return DeliveryService(state_path, peer_url=peer_url, samples_dir=samples_dir, now=now, **kwargs)


# 常见框架/测试习惯别名
build_service = create_service
make_service = create_service


class DeliveryHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, handler_class, service: DeliveryService,
                 reconcile_interval: float = 5.0):
        super().__init__(address, handler_class)
        self.service = service
        self.reconcile_interval = reconcile_interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start_background_reconcile(self) -> None:
        if self.reconcile_interval and self.reconcile_interval > 0:
            self._thread = threading.Thread(
                target=self._reconcile_loop, name="delivery-reconcile", daemon=True)
            self._thread.start()

    def _reconcile_loop(self) -> None:
        # 启动先对账一次：服务重启后继续未完成的撤销收敛
        while not self._stop_event.is_set():
            try:
                self.service.reconcile()
            except Exception:
                # 对账异常不能杀死循环，也不能改变未完成状态
                pass
            self._stop_event.wait(self.reconcile_interval)

    def shutdown(self) -> None:  # type: ignore[override]
        self._stop_event.set()
        super().shutdown()
        thread = self._thread
        if thread and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=2)


def create_server(host: str = "127.0.0.1", port: int = 8080, *,
                  state_path: "str | Path | None" = DEFAULT_STATE,
                  peer_url: str | None = DEFAULT_PEER_URL,
                  reconcile_interval: float = 5.0,
                  service: DeliveryService | None = None,
                  samples_dir: "str | Path | None" = None,
                  now=None) -> DeliveryHTTPServer:
    svc = service or create_service(state_path, peer_url=peer_url,
                                    samples_dir=samples_dir, now=now)
    handler = make_handler(svc)
    httpd = DeliveryHTTPServer((host, port), handler, svc, reconcile_interval)
    httpd.start_background_reconcile()
    return httpd


# 一些测试习惯里的名字
create_app = create_server
build_app = create_server


def json_default(value: Any) -> Any:
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    return str(value)


def make_handler(service: DeliveryService):
    class Handler(BaseHTTPRequestHandler):
        server_version = "DeliveryService/1.0"

        # ---- 基础收发 ---- #
        def _actor(self):
            actor_id = self.headers.get("X-Actor-Id")
            roles_header = self.headers.get("X-Actor-Role") or self.headers.get("X-Actor-Roles")
            roles = [r.strip() for r in roles_header.split(",") if r.strip()] if roles_header else None
            if not actor_id and not roles:
                return None  # 本机受信上下文
            return {"id": actor_id or "anonymous", "roles": roles or []}

        def _send_json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, ensure_ascii=False, default=json_default).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> Any:
            size = int(self.headers.get("Content-Length", 0) or 0)
            if size == 0:
                return {}
            if size > 4_194_304:
                raise ServiceError("请求体过大", code="body_too_large")
            try:
                return json.loads(self.rfile.read(size).decode("utf-8"))
            except ValueError as exc:
                raise ServiceError("请求体不是有效 JSON", code="bad_json") from exc

        def _send_html(self, status: int, html: str) -> None:
            body = html.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args) -> None:  # 不打印可能带参数的默认访问日志
            return

        # ---- 路由 ---- #
        def do_GET(self) -> None:  # noqa: N802
            self._dispatch("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch("POST")

        def do_DELETE(self) -> None:  # noqa: N802
            self._dispatch("DELETE")

        def _dispatch(self, method: str) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            if method == "GET" and path == "/":
                self._send_html(200, DASHBOARD_HTML)
                return
            query = {k: v[-1] for k, v in parse_qs(parsed.query, keep_blank_values=True).items()}
            try:
                body = self._read_json() if method == "POST" else {}
                if not isinstance(body, dict):
                    body = {"_body": body}
                result = route(service, method, path, query, body, self._actor())
                if result is None:
                    self._send_json(404, {"error": "not_found", "path": path})
                elif isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], int):
                    self._send_json(result[0], result[1])
                else:
                    self._send_json(200, result)
            except ServiceError as exc:
                self._send_json(exc.status, exc.to_dict())
            except Exception as exc:  # noqa: BLE001 - 边界兜底，不能让连接挂死
                self._send_json(500, {"error": "internal_error", "message": str(exc)})

    return Handler


def route(service: DeliveryService, method: str, path: str, query: dict,
          body: dict, actor: Any) -> Any:
    """纯路由函数，也方便不启 socket 的测试直接调用。"""

    b = dict(body or {})
    q = dict(query or {})
    merged = {**q, **b}

    # 允许 body 带 actor（脚本调用），统一成路由 actor，避免重复关键字
    body_actor = b.pop("actor", None)
    if actor is None and body_actor is not None:
        actor = body_actor
    merged["actor"] = actor

    if method == "GET" and path == "/health":
        return {"ok": True, "service": "cross-border-delivery", "time": format_time()}

    if path == "/rpc" and method == "POST":
        op = b.get("op")
        if op not in RPC_OPS:
            raise ServiceError(f"未知或不允许的 op: {op}", code="unknown_op")
        fn = getattr(service, op)
        raw_args = b.get("args") or {}
        if isinstance(raw_args, list):
            return fn(*raw_args)
        args = dict(raw_args)
        # 仅当目标方法接受 actor 时才注入调用方身份
        import inspect
        params = inspect.signature(fn).parameters
        if actor is not None and ("actor" in params or any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())):
            args.setdefault("actor", actor)
        return fn(**args)

    # 订单 ---------------------------------------------------------------- #
    if path == "/v1/orders" and method == "GET":
        return {"orders": list(service.store.data["deliveries"].values())}
    if path == "/v1/deliveries" and method == "POST":
        return service.report_delivery(actor=actor, **b)

    if path.startswith("/v1/orders/"):
        parts = path.split("/")
        # /v1/orders/{order} 或 .../choice|delegation
        order = parts[3] if len(parts) >= 4 else None
        sub = parts[4] if len(parts) >= 5 else None
        if not order:
            return None
        if sub is None and method == "GET":
            return service.get_delivery(order)
        if sub == "delegation" and method == "GET":
            return service.current_delegation(order, at=q.get("at"))
        if sub == "task":
            if method == "GET":
                return service.task_status(order)
            if method == "POST":
                payload = {k: v for k, v in b.items() if k != "order"}
                return service.advance_task(order, actor=actor, **payload)
        if sub == "choice" and method == "POST":
            payload = {k: v for k, v in b.items() if k != "order"}
            return service.update_choice(order, actor=actor, **payload)
        if sub == "receiver" and method == "POST":
            payload = {k: v for k, v in b.items() if k != "order"}
            return service.change_receiver(order, actor=actor, **payload)
        return None

    # 交付任务推进 -------------------------------------------------------- #
    if path == "/v1/tasks/event" and method == "POST":
        return service.advance_task(actor=actor, **b)
    if path == "/v1/tasks" and method == "GET":
        return {"task_events": service.task_events(order=q.get("order")),
                "task_status": service.store.data["task_status"]}

    # 调度 ---------------------------------------------------------------- #
    if path in {"/v1/dispatch", "/v1/grants"} and method == "POST":
        action = b.get("action")
        if path == "/v1/grants" and action is None:
            action = "delegate" if b.get("partner") and (b.get("parent_grant") or b.get("delegate")) else "assign"
        return service.record_dispatch(action, actor=actor, **{k: v for k, v in b.items() if k != "action"})

    # 运营方登记 ---------------------------------------------------------- #
    if path == "/v1/partners":
        if method == "POST":
            return service.register_partner(actor=actor, **b)
        if method == "GET":
            return {"partners": list(service.store.data["partners"].values())}
    if path == "/v1/contacts" and method == "POST":
        return service.upsert_contact(actor=actor, **b)

    # 资料查询/取号 -------------------------------------------------------- #
    if path in {"/v1/contact", "/v1/contact/view"} and method in {"GET", "POST"}:
        source = merged
        return service.view_contact(
            source.get("order"), source.get("grant"), source.get("partner"),
            source.get("fields"), at=source.get("at"), actor=actor)
    if path == "/v1/number" and method in {"GET", "POST"}:
        return service.take_number(merged.get("order"), merged.get("grant"),
                                   merged.get("partner"), at=merged.get("at"), actor=actor)
    if path == "/v1/scope" and method in {"GET", "POST"}:
        return service.scope_report(merged["order"], merged.get("partner"),
                                    merged.get("grant"), at=merged.get("at"))

    # 披露 ---------------------------------------------------------------- #
    if path == "/v1/disclosures" and method == "POST":
        return service.disclose(actor=actor, **b)
    if path == "/v1/disclosures" and method == "GET":
        return {"disclosures": service.list_disclosures(status=q.get("status"), order=q.get("order"))}
    if path.startswith("/v1/disclosures/"):
        rest = path[len("/v1/disclosures/"):]
        bits = rest.split("/")
        ref = bits[0]
        if len(bits) == 1 and method == "GET":
            return service.get_disclosure(ref)
        if len(bits) == 2 and bits[1] == "stop" and method == "POST":
            return service.request_stop(ref, actor=actor, **{k: v for k, v in b.items() if k != "actor"})
        return None

    # 对账 ---------------------------------------------------------------- #
    if path == "/v1/reconcile" and method == "POST":
        return service.reconcile(b.get("ref") or q.get("ref"))

    # 凭证 ---------------------------------------------------------------- #
    if path == "/v1/proofs" and method == "GET":
        return {"proofs": service.review_proofs(order=q.get("order"))}
    if path == "/v1/proofs" and method == "POST":
        return service.submit_proof(actor=actor, **b)
    if path.startswith("/v1/proofs/") and method == "GET":
        bits = path.split("/")
        receipt = bits[3]
        if len(bits) >= 5 and bits[4] == "dispatch":
            return service.dispatch_proof(
                receipt, grant=q.get("grant"), partner=q.get("partner"),
                at=q.get("at"), fields=q.get("fields"), actor=actor)
        return service.proof_validity(receipt)

    # 运营视图 ------------------------------------------------------------ #
    if path == "/v1/ops/overview" and method == "GET":
        return service.ops_overview(actor, live=q.get("live") in {"1", "true", "yes"})
    if path == "/v1/ops/followups" and method == "GET":
        return {"followups": service.followups(open_only=q.get("all") not in {"1", "true"})}
    if path.startswith("/v1/ops/followups/") and path.endswith("/resolve") and method == "POST":
        fid = path.split("/")[4]
        return service.resolve_followup(fid)

    # 审计流水 ------------------------------------------------------------ #
    if path in {"/v1/audit", "/v1/flow"} and method == "GET":
        return {"entries": service.audit_entries(action=q.get("action"), order=q.get("order"),
                                                 grant=q.get("grant"), ref=q.get("ref"))}

    return None


# 极简运营看板（无外部依赖）
DASHBOARD_HTML = """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>跨境交付授权与对账</title><style>
body{font:14px/1.5 -apple-system,"PingFang SC","Microsoft YaHei",sans-serif;margin:24px;color:#1b2733}
h1{font-size:20px} h2{font-size:16px;margin-top:28px}
table{border-collapse:collapse;width:100%;margin:8px 0} th,td{border:1px solid #d7dee6;padding:6px 8px;text-align:left;vertical-align:top}
th{background:#f2f5f8} .ok{color:#0a7d33;font-weight:600}.bad{color:#b3261e;font-weight:600}.pend{color:#9a6700;font-weight:600}
code{background:#f2f5f8;padding:1px 5px;border-radius:4px} .muted{color:#5c6b7a}
</style></head><body>
<h1>跨境交付联络权限与履约回执</h1>
<p class="muted">本页只展示作业与对账信息；流水不含个人资料原值。数据每 5 秒刷新。</p>
<h2>运营总览</h2><pre id="ov">加载中…</pre>
<h2>当前委托（X3）</h2><pre id="del">加载中…</pre>
<script>
async function j(u){const r=await fetch(u);return r.json();}
function tag(s){if(s==='stopped'||s===true||s==='valid')return '<span class=ok>'+s+'</span>';
 if(s==='stop_pending'||s==='active')return '<span class=pend>'+s+'</span>';return '<span class=bad>'+s+'</span>';}
async function tick(){
 try{const ov=await j('/v1/ops/overview');
  document.getElementById('ov').textContent=JSON.stringify({
   valid_proofs:ov.valid_proofs.map(p=>p.receipt),
   invalid_proofs:ov.invalid_proofs.map(p=>[p.receipt,p.validity.reason]),
   pending_convergence:ov.pending_convergence.map(d=>[d.ref,d.peer_status,d.stop_attempts]),
   followups:ov.followups.map(f=>[f.id,f.kind,f.subject,f.summary])},null,2);
  const del=await j('/v1/orders/X3/delegation');
  document.getElementById('del').textContent=JSON.stringify(del,null,2);
 }catch(e){document.getElementById('ov').textContent=String(e);}
}
tick();setInterval(tick,5000);
</script></body></html>"""


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="跨境交付联络权限与履约回执服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=int(os.environ.get("DELIVERY_PORT", "8080")))
    parser.add_argument("--state", default=str(DEFAULT_STATE), help="状态 JSON 路径（重启续跑）")
    parser.add_argument("--peer-url", default=DEFAULT_PEER_URL)
    parser.add_argument("--reconcile-interval", type=float, default=5.0)
    parser.add_argument("--as-of", default=None, help="演练用固定当前时间（ISO/Z）")
    options = parser.parse_args(argv)

    httpd = create_server(options.host, options.port, state_path=options.state,
                          peer_url=options.peer_url,
                          reconcile_interval=options.reconcile_interval, now=options.as_of)
    print(f"交付服务监听 http://{options.host}:{httpd.server_port}", flush=True)
    print(f"状态文件 {options.state}；合作端 {options.peer_url}", flush=True)
    try:
        httpd.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
        httpd.server_close()
        httpd.service.close()


if __name__ == "__main__":
    main()
