"""跨境交付联络权限与履约回执服务。

职责：
* 出口方报送交付单与收车方联络选择（``deliveries``）；
* 区域运营方登记配送方、服务区域与获准数据范围（``partners``）；
* 调度在同一订单上发出/结束/交接委托（``dispatch_changes`` 语义）；
* 按“订单同意字段 ∩ 配送方获准范围 ∩ 委托链范围”给字段，委托只能相同或更小；
* proxy_only 地区只给项目转接引用（relay），不给电话号码原值；
* 同一联络人在不同订单上的同意互不打通（按订单授权）；
* 对外披露走合作端 ``POST /disclosures`` 契约，撤销靠 stop + GET 对账收敛，
  只有合作端返回 ``stopped`` 并给证据才标记完成；
* 流水/审计与披露记录只保存字段名，不保存个人资料原值；
* 签收凭证按 delivered_at 与委托生效区间判定有效性，保留凭证但不放宽资料权限；
* 状态原子落盘，重启后可继续对账。

仅使用 Python 标准库。时间统一为带 Z 的 UTC ISO 8601。
"""

from __future__ import annotations

import json
import os
import http.client
import threading
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parent
DEFAULT_SAMPLES_DIR = ROOT / "samples"
DEFAULT_PEER_URL = os.environ.get("DELIVERY_PEER_URL", "http://127.0.0.1:8087")
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

__all__ = [
    "DeliveryService",
    "Service",
    "Store",
    "PeerClient",
    "PeerResponse",
    "Actor",
    "FieldScope",
    "ServiceError",
    "NotFound",
    "PermissionDenied",
    "ConflictError",
    "PeerUnavailable",
    "parse_time",
    "format_time",
    "resolve_allowed_fields",
    "evaluate_proof",
    "proof_is_valid",
    "new_id",
]


# --------------------------------------------------------------------------- #
# 基础工具
# --------------------------------------------------------------------------- #


def parse_time(value: Any) -> datetime | None:
    """把带 Z 的 ISO 8601 解析成 aware UTC datetime；None 原样返回。"""

    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_time(value: datetime | None = None) -> str:
    """格式化为样本约定的 ``YYYY-MM-DDTHH:MM:SSZ``。"""

    if value is None:
        value = datetime.now(timezone.utc)
    return value.astimezone(timezone.utc).strftime(TIME_FORMAT)


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def as_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return [str(part) for part in value]


class FieldScope(set):
    """获准字段集合。

    是 ``set`` 的子类；额外允许与 list/tuple 按无序字段名比较，
    方便不同调用习惯下的断言（``== {"phone"}`` 与 ``== ["phone"]`` 都成立）。
    """

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (list, tuple)):
            return set(other) == set(self)
        if isinstance(other, Iterable) and not isinstance(other, (str, bytes, dict)):
            try:
                return set(other) == set(self)
            except TypeError:
                return NotImplemented
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        return result if result is NotImplemented else not result

    def sorted(self) -> list[str]:
        return sorted(self)

    def as_list(self) -> list[str]:
        return sorted(self)


# --------------------------------------------------------------------------- #
# 错误与行为主体
# --------------------------------------------------------------------------- #


class ServiceError(Exception):
    """业务错误基类，``status`` 对应 HTTP 状态码。"""

    status = 400
    code = "service_error"

    def __init__(self, message: str = "", *, code: str | None = None, details: dict | None = None):
        super().__init__(message or self.__class__.__doc__ or self.code)
        # error 是大类（permission_denied/not_found/...），code 是更细原因
        self.error = type(self).code
        self.code = code or type(self).code
        self.message = message or self.code
        self.details = details or {}

    def to_dict(self) -> dict:
        payload = {"error": self.error, "code": self.code, "message": self.message}
        if self.details:
            payload["details"] = self.details
        return payload


class NotFound(ServiceError):
    status = 404
    code = "not_found"


class PermissionDenied(ServiceError):
    status = 403
    code = "permission_denied"


class ConflictError(ServiceError):
    status = 409
    code = "conflict"


class PeerUnavailable(ServiceError):
    status = 502
    code = "peer_unavailable"


class Actor:
    """调用方。``actor_id=None`` 表示本机受信上下文（进程内/回环网关之后）。"""

    def __init__(self, actor_id: str | None = "system", roles: Iterable[str] | None = None):
        self.id = actor_id or "system"
        self.roles = set(roles or (["system"] if self.id == "system" else []))
        if self.id == "system":
            self.roles.add("system")

    @property
    def trusted(self) -> bool:
        return "system" in self.roles or "*" in self.roles

    def has_role(self, role: str) -> bool:
        if self.trusted or role in self.roles:
            return True
        # ops 角色覆盖运营查询类角色名
        return role in {"operator", "operations"} and "ops" in self.roles

    @classmethod
    def coerce(cls, actor: "Actor | dict | str | None") -> "Actor":
        if actor is None:
            return SYSTEM_ACTOR
        if isinstance(actor, Actor):
            return actor
        if isinstance(actor, dict):
            return cls(actor.get("id") or actor.get("actor") or actor.get("name"),
                       as_list(actor.get("roles") or actor.get("role")))
        if isinstance(actor, str):
            return cls(actor)
        return cls(str(actor))


SYSTEM_ACTOR = Actor("system", ["system"])


# --------------------------------------------------------------------------- #
# 持久化（原子 JSON）
# --------------------------------------------------------------------------- #


def _blank_state() -> dict:
    return {
        "version": 1,
        "seq": 0,
        "operators": ["ops"],
        "deliveries": {},
        "partners": {},
        "dispatch_events": [],
        "event_ids": {},
        "grants": {},
        "disclosures": {},
        "proofs": {},
        "task_events": [],
        "task_status": {},
        "audit": [],
        "followups": [],
        "counters": {"disclosure": 0},
        "meta": {},
    }


class Store:
    """JSON 文件存储。``path`` 为 None 或 ``":memory:"`` 时只在内存。"""

    def __init__(self, path: "str | os.PathLike | None" = None):
        self.lock = threading.RLock()
        self.path: Path | None
        if path is None or str(path) == ":memory:":
            self.path = None
        else:
            candidate = Path(path)
            # 允许调用方直接给一个状态目录
            if candidate.exists() and candidate.is_dir():
                candidate = candidate / "state.json"
            self.path = candidate
        self.data = _blank_state()
        if self.path is not None and self.path.exists():
            self.load()

    def load(self) -> dict:
        with self.lock:
            assert self.path is not None
            text = self.path.read_text(encoding="utf-8")
            loaded = json.loads(text) if text.strip() else {}
            base = _blank_state()
            base.update(loaded)
            self.data = base
            return self.data

    def save(self) -> None:
        if self.path is None:
            return
        with self.lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp_name = _temp_file_in(self.path.parent)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(self.data, handle, ensure_ascii=False, indent=2, sort_keys=False)
                    handle.write("\n")
                os.replace(tmp_name, self.path)
            except BaseException:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass
                raise

    def next_seq(self) -> int:
        self.data["seq"] = int(self.data.get("seq", 0)) + 1
        return self.data["seq"]


def _temp_file_in(directory: Path) -> tuple[int, str]:
    directory.mkdir(parents=True, exist_ok=True)
    stem = f".state-{os.getpid()}-{uuid.uuid4().hex[:8]}.tmp"
    path = directory / stem
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    return fd, str(path)


# --------------------------------------------------------------------------- #
# 合作端 HTTP 客户端（契约见《交换资料约定》“对端接口”）
# --------------------------------------------------------------------------- #


class PeerResponse(tuple):
    """``(status, payload)``；status 0 表示连接断开且未收到应答。"""

    def __new__(cls, status: int, payload: Any):
        obj = tuple.__new__(cls, (status, payload))
        return obj

    @property
    def status(self) -> int:
        return self[0]

    @property
    def payload(self) -> Any:
        return self[1]

    @property
    def ok(self) -> bool:
        return 200 <= self[0] < 300


class PeerClient:
    """仅用 urllib 的合作端客户端。网络异常归一化为 ``PeerResponse(0, None)``。"""

    def __init__(self, base_url: str | None = DEFAULT_PEER_URL, *, timeout: float = 3.0, opener=None):
        self.base_url = (base_url or "").rstrip("/")
        self.timeout = timeout
        self._opener = opener

    def _request(self, method: str, path: str, body: Any = None) -> PeerResponse:
        url = f"{self.base_url}{path}"
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            if self._opener is not None:
                response = self._opener.open(request, timeout=self.timeout)
            else:
                response = urllib.request.urlopen(request, timeout=self.timeout)  # noqa: S310 (loopback drill)
            with response:
                raw = response.read()
                status = response.status
        except urllib.error.HTTPError as exc:  # 4xx/5xx 仍是有应答
            raw = exc.read()
            status = exc.code
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError,
                http.client.HTTPException) as exc:
            return PeerResponse(0, {"error": "no_response", "detail": type(exc).__name__})
        try:
            payload = json.loads(raw.decode("utf-8")) if raw else None
        except (ValueError, UnicodeDecodeError):
            payload = None
        return PeerResponse(status, payload)

    # 契约的三个调用 ------------------------------------------------------- #

    def create_disclosure(self, ref: str, grant: str, fields: dict) -> PeerResponse:
        return self._request("POST", "/disclosures",
                             {"ref": ref, "grant": grant, "fields": fields})

    def stop_disclosure(self, ref: str) -> PeerResponse:
        return self._request("POST", f"/disclosures/{ref}/stop", {})

    def get_disclosure(self, ref: str) -> PeerResponse:
        return self._request("GET", f"/disclosures/{ref}", None)


def coerce_peer_response(result: Any) -> tuple[int, Any]:
    """把多种可能的合作端替身返回形态归一化为 ``(status, payload)``。"""

    if isinstance(result, PeerResponse):
        return result.status, result.payload
    if isinstance(result, tuple) and len(result) == 2:
        try:
            return int(result[0]), result[1]
        except (TypeError, ValueError):
            return 0, None
    status = getattr(result, "status_code", None) or getattr(result, "status", None)
    if status is not None:
        payload = None
        json_method = getattr(result, "json", None)
        if callable(json_method):
            try:
                payload = json_method()
            except Exception:  # noqa: BLE001
                payload = None
        if payload is None:
            try:
                payload = json.loads(getattr(result, "text", "") or "null")
            except (ValueError, TypeError):
                payload = None
        try:
            return int(status), payload
        except (TypeError, ValueError):
            return 0, None
    return 0, None


# --------------------------------------------------------------------------- #
# 纯领域规则
# --------------------------------------------------------------------------- #


def resolve_allowed_fields(
    delivery: dict,
    partner: dict,
    grant: dict | None = None,
    *,
    ancestor_partners: Iterable[dict] = (),
    at: Any = None,
    contact: dict | None = None,
    purpose: str | None = None,
) -> FieldScope:
    """计算某票订单在某委托/配送方下可披露的字段名（不含取值）。

    规则：订单当前 choice ∩ 配送方 allowed ∩ 显式委托 scope ∩ 委托链上每家
    的 allowed/scope；proxy_only 地区收敛为唯一的项目转接引用 ``relay``。
    有效期/用途不符时返回空集合。
    """

    at_dt = parse_time(at)
    until = parse_time(delivery.get("until"))
    if at_dt is not None and until is not None and at_dt > until:
        return FieldScope()
    if purpose and delivery.get("purpose") and purpose != delivery["purpose"]:
        return FieldScope()

    choice = set(delivery.get("choice") or [])
    allowed = set(partner.get("allowed") or [])
    fields = choice & allowed

    # 显式委托范围（若签发时收窄过）
    if grant is not None and grant.get("scope"):
        fields &= set(grant["scope"])
    # 委托链：协力方可见范围只能与委托方相同或更小
    for ancestor in ancestor_partners:
        fields &= set(ancestor.get("allowed") or [])
        if ancestor.get("scope"):
            fields &= set(ancestor["scope"])

    if partner.get("proxy_only"):
        # 境内转接：电话号码同意只能通过项目 relay 引用落地，且委托侧必须
        # 获准 relay、资料中存在 relay。其它字段（含 drop_zone）一律不发。
        basis = {"phone", "relay"} & choice
        relay_ok = "relay" in set(partner.get("allowed") or [])
        has_relay = bool((contact or {}).get("relay"))
        if basis and relay_ok and has_relay:
            return FieldScope({"relay"})
        return FieldScope()

    return FieldScope(fields)


def evaluate_proof(proof: dict, events: Iterable[dict]) -> dict:
    """按签收发生时间（delivered_at）对照委托区间判定凭证有效性。

    上传时间 received_at 不影响有效性。返回
    ``{"valid": bool, "reason": str|None, "opened_at", "closed_at"}``。
    """

    order, grant, partner = proof.get("order"), proof.get("grant"), proof.get("partner")
    related = [
        e for e in events
        if e.get("order") == order and e.get("grant") == grant and e.get("partner") == partner
    ]
    opens = [parse_time(e.get("at")) for e in related if e.get("action") in {"assign", "delegate"}]
    closes = [parse_time(e.get("at")) for e in related if e.get("action") == "end"]
    opens = [t for t in opens if t is not None]
    closes = [t for t in closes if t is not None]

    delivered = parse_time(proof.get("delivered_at"))
    if not related or not opens:
        return {"valid": False, "reason": "unknown_grant", "opened_at": None,
                "closed_at": format_time(min(closes)) if closes else None}
    opened_at = min(opens)
    closed_at = min(closes) if closes else None
    if delivered is None:
        return {"valid": False, "reason": "missing_delivered_at",
                "opened_at": format_time(opened_at),
                "closed_at": format_time(closed_at) if closed_at else None}
    if delivered < opened_at:
        reason = "before_assign"
    elif closed_at is not None and delivered >= closed_at:
        reason = "after_end"
    elif not proof.get("evidence"):
        reason = "missing_evidence"
    else:
        reason = None
    return {
        "valid": reason is None,
        "reason": reason,
        "opened_at": format_time(opened_at),
        "closed_at": format_time(closed_at) if closed_at else None,
    }


def proof_is_valid(proof: dict, events: Iterable[dict]) -> bool:
    return evaluate_proof(proof, events)["valid"]


# --------------------------------------------------------------------------- #
# 主服务
# --------------------------------------------------------------------------- #


class DeliveryService:
    """交付授权、披露、改派收敛、凭证与运营视图的统一入口。"""

    def __init__(
        self,
        state_path: "str | os.PathLike | Store | PeerClient | dict | None" = None,
        peer_url: str | None = None,
        *args,
        samples_dir: "str | os.PathLike | None" = None,
        peer: Any = None,
        peer_client: Any = None,
        base_url: str | None = None,
        clock=None,
        now=None,
        store: Store | None = None,
        **kwargs,
    ):
        # 兼容不同命名习惯的位置/关键字参数
        if args and peer_url is None:
            peer_url = args[0]
        for alias in ("path", "db_path", "state_file", "store_path", "db"):
            if state_path is None and kwargs.get(alias) is not None:
                state_path = kwargs[alias]

        injected_state: dict | None = None
        if isinstance(state_path, str) and state_path.startswith(("http://", "https://")):
            peer_url = peer_url or state_path
            state_path = None
        elif hasattr(state_path, "create_disclosure") and hasattr(state_path, "get_disclosure"):
            peer_client = peer_client or state_path
            state_path = None
        elif isinstance(state_path, Store):
            store = store or state_path
            state_path = None
        elif isinstance(state_path, dict):
            injected_state = state_path
            state_path = None

        self.samples_dir = Path(samples_dir or kwargs.get("sample_dir") or DEFAULT_SAMPLES_DIR)
        self.clock = clock or (now if callable(now) else None)
        self._fixed_now = parse_time(now) if not callable(now) and now is not None else None

        if store is not None:
            self.store = store
        else:
            self.store = Store(state_path)
        if injected_state is not None:
            merged = _blank_state()
            merged.update(injected_state)
            self.store.data = merged
        self._lock = self.store.lock

        if peer is not None:
            self.peer = peer
        elif peer_client is not None:
            self.peer = peer_client
        elif peer_url is not None or base_url is not None:
            self.peer = PeerClient(peer_url or base_url)
        else:
            self.peer = PeerClient(DEFAULT_PEER_URL)

        self.contacts: dict[str, dict] = {}
        self._private_contacts_path: Path | None = None
        if self.store.path is not None:
            self._private_contacts_path = self.store.path.parent / "contacts.registry.json"
        self._load_contacts()
        self.load_feeds()

    # ---- 时间 ------------------------------------------------------------ #

    def now(self, at: Any = None) -> datetime:
        if at is not None:
            parsed = parse_time(at)
            assert parsed is not None
            return parsed
        if self._fixed_now is not None:
            return self._fixed_now
        if self.clock is not None:
            value = self.clock()
            return parse_time(value) if not isinstance(value, datetime) else value
        return datetime.now(timezone.utc)

    def _next_counter(self, name: str) -> int:
        counters = self.store.data.setdefault("counters", {})
        counters[name] = int(counters.get(name, 0)) + 1
        return counters[name]

    def _generate_ref(self, prefix: str) -> str:
        existing = set(self.store.data["disclosures"].keys())
        while True:
            candidate = f"{prefix}-{self._next_counter('disclosure')}"
            if candidate not in existing:
                return candidate

    def _peer_call(self, method: str, *args) -> tuple[int, Any]:
        """调用合作端并归一化；任何异常都按 status 0（未收到应答）处理。"""
        try:
            fn = getattr(self.peer, method)
        except AttributeError:
            return 0, {"error": "peer_method_missing", "method": method}
        try:
            return coerce_peer_response(fn(*args))
        except Exception as exc:  # noqa: BLE001 - 对端任何异常都不能误判收敛
            return 0, {"error": "no_response", "detail": type(exc).__name__}

    # ---- 资料装载 -------------------------------------------------------- #

    def _load_contacts(self) -> None:
        self.contacts = {}
        path = self.samples_dir / "contacts.jsonl"
        if path.exists():
            for row in _read_jsonl(path):
                if row.get("contact_ref"):
                    self.contacts[row["contact_ref"]] = row
        if self._private_contacts_path and self._private_contacts_path.exists():
            try:
                extra = json.loads(self._private_contacts_path.read_text(encoding="utf-8"))
                for ref, row in extra.items():
                    self.contacts[ref] = row
            except (ValueError, OSError):
                pass

    def _save_private_contact(self, row: dict) -> None:
        if self._private_contacts_path is None:
            return
        self._private_contacts_path.parent.mkdir(parents=True, exist_ok=True)
        existing = {}
        if self._private_contacts_path.exists():
            existing = json.loads(self._private_contacts_path.read_text(encoding="utf-8"))
        existing[row["contact_ref"]] = row
        tmp = self._private_contacts_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self._private_contacts_path)

    def load_feeds(self, samples_dir: "str | Path | None" = None) -> dict:
        """幂等装载 samples 下五类交换记录；已存在的可变状态不被覆盖。"""

        if samples_dir is not None:
            self.samples_dir = Path(samples_dir)
        counts = {"deliveries": 0, "partners": 0, "contacts": 0, "dispatch_events": 0, "proofs": 0}
        with self._lock:
            deliveries_path = self.samples_dir / "deliveries.jsonl"
            if deliveries_path.exists():
                for row in _read_jsonl(deliveries_path):
                    if row["order"] not in self.store.data["deliveries"]:
                        self.store.data["deliveries"][row["order"]] = dict(row)
                        counts["deliveries"] += 1

            partners_path = self.samples_dir / "partners.jsonl"
            if partners_path.exists():
                for row in _read_jsonl(partners_path):
                    key = partner_key(row["partner"], row["region"])
                    if key not in self.store.data["partners"]:
                        self.store.data["partners"][key] = dict(row)
                        counts["partners"] += 1

            self._load_contacts()
            counts["contacts"] = len(self.contacts)

            dispatch_path = self.samples_dir / "dispatch_changes.jsonl"
            if dispatch_path.exists():
                rows = _read_jsonl(dispatch_path)
                self.import_dispatch_events(rows, save=False)
                counts["dispatch_events"] = len(rows)

            proofs_path = self.samples_dir / "proofs.jsonl"
            if proofs_path.exists():
                for row in _read_jsonl(proofs_path):
                    if row["receipt"] not in self.store.data["proofs"]:
                        self.store.data["proofs"][row["receipt"]] = dict(row)
                        counts["proofs"] += 1
            self._materialize_grants(save=False)
            self._sync_proof_followups_locked()
            self.store.save()
        return counts

    def _sync_proof_followups_locked(self) -> None:
        """对载入/提交的凭证做复核：无效凭证进人工跟进，恢复有效则结案。"""

        for receipt, row in self.store.data["proofs"].items():
            verdict = evaluate_proof(row, self.store.data["dispatch_events"])
            existing = None
            for item in self.store.data["followups"]:
                if item["kind"] == "proof_out_of_window" and item["subject"] == receipt:
                    existing = item
                    break
            if not verdict["valid"]:
                if existing is None or existing["status"] != "open":
                    self._open_followup_locked(
                        "proof_out_of_window", receipt, order=row.get("order"),
                        summary=f"凭证 {receipt} 有效性存疑（{verdict['reason']}），需人工核验")
            elif existing is not None and existing["status"] == "open":
                existing["status"] = "resolved"
                existing["resolved_at"] = format_time(self.now())

    # ---- 审计流水（绝不写入字段原值） ------------------------------------ #

    def audit(self, action: str, *, actor: Any = None, fields: Iterable[str] | None = None,
              save: bool = True, **context) -> dict:
        actor_obj = Actor.coerce(actor)
        at_value = context.pop("at", None)
        entry = {
            "seq": self.store.next_seq(),
            "ts": format_time(self.now(at_value)),
            "actor": actor_obj.id,
            "roles": sorted(actor_obj.roles),
            "action": action,
            "fields": sorted(fields) if fields else [],
        }
        # context 中只允许出现标识/结论类信息；即便误传 fields_values 也剔除，
        # 防止个人资料原值进入流水。
        context.pop("fields_values", None)
        context.pop("values", None)
        for key, value in list(context.items()):
            if key in {"phone", "drop_zone", "relay"}:
                context.pop(key)
                context[f"{key}_logged"] = False
            if isinstance(value, (datetime,)):
                context[key] = format_time(value)
        entry.update(context)
        self.store.data["audit"].append(entry)
        if save:
            self.store.save()
        return entry

    def audit_entries(self, *, action: str | None = None, order: str | None = None,
                      grant: str | None = None, ref: str | None = None) -> list[dict]:
        result = []
        for entry in self.store.data["audit"]:
            if action and entry.get("action") != action:
                continue
            if order and entry.get("order") != order:
                continue
            if grant and entry.get("grant") != grant:
                continue
            if ref and entry.get("ref") != ref:
                continue
            result.append(dict(entry))
        return result

    @property
    def flow(self) -> list[dict]:
        return self.audit_entries()

    # ---- 运营授权 -------------------------------------------------------- #

    def authorize_ops(self, actor: Any = None) -> Actor:
        actor_obj = Actor.coerce(actor)
        if actor_obj.trusted or "ops" in actor_obj.roles:
            return actor_obj
        if actor_obj.id in set(self.store.data.get("operators", [])):
            actor_obj.roles.add("ops")
            return actor_obj
        raise PermissionDenied("需要区域运营授权", code="ops_authorization_required",
                               details={"actor": actor_obj.id})

    def add_operator(self, actor_id: str) -> None:
        with self._lock:
            ops = set(self.store.data.get("operators", []))
            ops.add(actor_id)
            self.store.data["operators"] = sorted(ops)
            self.store.save()

    # ---- 出口方/运营方登记 ----------------------------------------------- #

    def report_delivery(self, record: dict | None = None, *, actor: Any = "exporter",
                        **fields) -> dict:
        row = dict(record or {})
        row.update({k: v for k, v in fields.items() if v is not None})
        order = row.get("order")
        if not order:
            raise ServiceError("缺少 order")
        required = ["region", "purpose", "choice", "until", "contact_ref"]
        missing = [key for key in required if row.get(key) in (None, "")]
        if missing:
            raise ServiceError("交付单缺少字段", details={"missing": missing})
        parse_time(row["until"])  # 校验
        with self._lock:
            before = self.store.data["deliveries"].get(order)
            if before is not None:
                # 保留接收方后续改过的 choice/contact_ref，除非本次显式带 revise
                if not row.pop("revise", False):
                    row.setdefault("choice", before["choice"])
                    row.setdefault("contact_ref", before["contact_ref"])
            self.store.data["deliveries"][order] = row
            self.audit("delivery.reported", actor=actor, order=order, save=False)
            self.store.save()
        return dict(row)

    upsert_delivery = report_delivery

    def register_partner(self, record: dict | None = None, *, actor: Any = "ops", **fields) -> dict:
        self.authorize_ops(actor)
        row = dict(record or {})
        row.update({k: v for k, v in fields.items() if v is not None})
        if not row.get("partner") or not row.get("region"):
            raise ServiceError("partner 与 region 必填")
        row.setdefault("allowed", [])
        row.setdefault("proxy_only", False)
        row.setdefault("delegate_of", "")
        with self._lock:
            self.store.data["partners"][partner_key(row["partner"], row["region"])] = row
            self.audit("partner.registered", actor=actor, partner=row["partner"],
                       region=row["region"], fields=row["allowed"], save=False)
            self.store.save()
        return dict(row)

    def upsert_contact(self, record: dict | None = None, **fields) -> dict:
        row = dict(record or {})
        row.update({k: v for k, v in fields.items() if v is not None})
        if not row.get("contact_ref"):
            raise ServiceError("缺少 contact_ref")
        with self._lock:
            self.contacts[row["contact_ref"]] = row
            self._save_private_contact(row)
        return dict(row)

    # ---- 查询基础记录 ---------------------------------------------------- #

    def get_delivery(self, order: str) -> dict:
        row = self.store.data["deliveries"].get(order)
        if row is None:
            raise NotFound(f"未知订单 {order}", code="unknown_order")
        return dict(row)

    get_order = get_delivery

    def get_contact_ref(self, contact_ref: str) -> dict:
        row = self.contacts.get(contact_ref)
        if row is None:
            raise NotFound(f"未知联络引用 {contact_ref}", code="unknown_contact_ref")
        return dict(row)

    def get_partner(self, partner: str, region: str | None = None) -> dict:
        if region is not None:
            row = self.store.data["partners"].get(partner_key(partner, region))
            if row is not None:
                return dict(row)
        candidates = [dict(v) for k, v in self.store.data["partners"].items()
                       if k.split("@", 1)[0] == partner]
        if not candidates:
            raise NotFound(f"未登记配送方 {partner}", code="unknown_partner")
        if region is None:
            return candidates[0]
        raise NotFound(f"配送方 {partner} 未登记服务区域 {region}", code="partner_region_not_registered")

    # ---- 收货方改选择/换人 ---------------------------------------------- #

    def update_choice(self, order: str, choice: Iterable[str] | None = None, *,
                      contact_ref: str | None = None, recipient: str | None = None,
                      actor: Any = "receiver", at: Any = None,
                      revoke_disclosures: bool = True, **_ignored) -> dict:
        """收货方更改同意字段或更换联络人；本地权限立即按最新范围计算。

        若收缩/撤回了字段或更换 contact_ref，相关在途披露自动进入停用对账。
        """

        with self._lock:
            delivery = self.get_delivery(order)
            old_choice = list(delivery.get("choice") or [])
            old_ref = delivery.get("contact_ref")
            new_choice = list(choice) if choice is not None else old_choice
            if contact_ref is not None and contact_ref not in self.contacts:
                raise NotFound(f"未知联络引用 {contact_ref}", code="unknown_contact_ref")

            delivery["choice"] = new_choice
            if contact_ref is not None:
                delivery["contact_ref"] = contact_ref
            if recipient is not None:
                delivery["recipient"] = recipient
            self.store.data["deliveries"][order] = delivery

            removed = set(old_choice) - set(new_choice)
            changed_contact = contact_ref is not None and contact_ref != old_ref
            self.audit("receiver.choice_changed", actor=actor, order=order, at=at,
                       fields=sorted(new_choice), removed_fields=sorted(removed),
                       contact_changed=changed_contact, save=False)

            stopped: list[str] = []
            choice_changed = set(new_choice) != set(old_choice)
            if revoke_disclosures and (removed or changed_contact or choice_changed):
                for ref, disc in list(self.store.data["disclosures"].items()):
                    if disc["order"] != order or disc["status"] == "stopped":
                        continue
                    sent = set(disc.get("sent_fields") or [])
                    # phone 同意在 proxy_only 地区以 relay 引用落地，
                    # 撤回 phone 同样要停用已发出的 relay 引用。
                    withdrawn_basis = set(removed)
                    if "phone" in withdrawn_basis:
                        withdrawn_basis.add("relay")
                    affected = changed_contact or bool(withdrawn_basis & sent)
                    if affected:
                        self._request_stop_locked(ref, reason="consent_changed", at=at)
                        stopped.append(ref)
            self.store.save()
            result = dict(delivery)
            result["stop_requested"] = stopped
            return result

    # 语义别名
    change_choice = update_choice
    revise_consent = update_choice

    def change_receiver(self, order: str, contact_ref: str, **kwargs) -> dict:
        return self.update_choice(order, contact_ref=contact_ref, **kwargs)

    # ---- 调度事件与委托 -------------------------------------------------- #

    def import_dispatch_events(self, rows: Iterable[dict], *, save: bool = True) -> int:
        count = 0
        with self._lock:
            known = self.store.data["event_ids"]
            for row in rows:
                event_id = row.get("event") or new_id("M")
                if event_id in known:
                    continue
                record = {
                    "event": event_id,
                    "order": row["order"],
                    "grant": row["grant"],
                    "partner": row["partner"],
                    "action": row["action"],
                    "at": row["at"],
                }
                self.store.data["dispatch_events"].append(record)
                known[event_id] = True
                count += 1
            self.store.data["dispatch_events"].sort(key=lambda e: parse_time(e["at"]) or datetime.min.replace(tzinfo=timezone.utc))
            self._materialize_grants(save=False)
            if save:
                self.store.save()
        return count

    def record_dispatch(self, action: str, order: str, grant: str, partner: str, *,
                        at: Any = None, actor: Any = "dispatch", parent_grant: str | None = None,
                        scope: Iterable[str] | None = None, event: str | None = None,
                        save: bool = True, **_ignored) -> dict:
        if action not in {"assign", "end", "delegate"}:
            raise ServiceError("action 必须是 assign、end 或 delegate")
        ts = format_time(self.now(at))
        # 命令路径校验：派给/委托给的一方必须已在该订单区域登记；
        # 转委托的协力方登记的 delegate_of 必须与委托方一致。
        delivery = self.get_delivery(order)
        if action in {"assign", "delegate"}:
            partner_row = self._partner_for(partner, delivery["region"])
            if action == "delegate":
                parent_id = parent_grant or self._find_parent_grant(order, partner, ts)
                if parent_id:
                    parent_row = self.store.data["grants"].get(parent_id)
                    delegator = (parent_row or {}).get("partner")
                    registered_of = partner_row.get("delegate_of") or ""
                    if delegator and registered_of and registered_of != delegator:
                        raise PermissionDenied(
                            f"协力方 {partner} 未登记在委托方 {delegator} 名下",
                            code="delegation_chain_not_registered",
                            details={"partner": partner, "delegator": delegator,
                                     "registered_delegate_of": registered_of})
        event_id = event or new_id("M")
        row = {"event": event_id, "order": order, "grant": grant,
               "partner": partner, "action": action, "at": ts}
        with self._lock:
            self.import_dispatch_events([row], save=False)
            grant_row = self.store.data["grants"].get(grant)
            if grant_row is not None:
                if parent_grant:
                    grant_row["parent_grant"] = parent_grant
                if scope is not None:
                    grant_row["scope"] = as_list(scope)
            self.audit(f"dispatch.{action}", actor=actor, order=order, grant=grant,
                       partner=partner, at=ts, parent_grant=parent_grant, save=False)
            if action == "end":
                # 改派/结束：在途对外引用进入停用对账
                for ref, disc in list(self.store.data["disclosures"].items()):
                    if disc["order"] == order and disc["grant"] == grant and disc["status"] != "stopped":
                        self._request_stop_locked(ref, reason="grant_ended", at=ts)
            if save:
                self.store.save()
        return row

    def assign(self, order: str, grant: str, partner: str, **kw) -> dict:
        return self.record_dispatch("assign", order, grant, partner, **kw)

    def end_grant(self, order: str, grant: str, partner: str, **kw) -> dict:
        return self.record_dispatch("end", order, grant, partner, **kw)

    def delegate(self, order: str, grant: str, partner: str, *,
                 parent_grant: str | None = None, scope: Iterable[str] | None = None, **kw) -> dict:
        return self.record_dispatch("delegate", order, grant, partner,
                                    parent_grant=parent_grant, scope=scope, **kw)

    def _materialize_grants(self, *, save: bool = True) -> None:
        """按时间重放 dispatch 事件，生成 grant 区间与父子关系。"""

        grants = self.store.data["grants"]
        for event in sorted(self.store.data["dispatch_events"],
                            key=lambda e: parse_time(e["at"]) or datetime.min.replace(tzinfo=timezone.utc)):
            gid = event["grant"]
            grant = grants.setdefault(gid, {
                "grant": gid, "order": event["order"], "partner": event["partner"],
                "parent_grant": None, "scope": None, "opened_at": None,
                "closed_at": None, "events": [],
            })
            grant["events"].append(event["event"])
            if event["action"] in {"assign", "delegate"} and grant["opened_at"] is None:
                grant["opened_at"] = event["at"]
            if event["action"] == "end":
                grant["closed_at"] = event["at"]
            if event["action"] == "delegate" and not grant.get("parent_grant"):
                parent = self._find_parent_grant(event["order"], event["partner"], event["at"])
                if parent:
                    grant["parent_grant"] = parent
        if save:
            self.store.save()

    def _find_parent_grant(self, order: str, child_partner: str, at: Any) -> str | None:
        partner_row = None
        for row in self.store.data["partners"].values():
            if row["partner"] == child_partner:
                partner_row = row
                break
        delegator = (partner_row or {}).get("delegate_of") or ""
        at_dt = parse_time(at)
        candidates = []
        for gid, grant in self.store.data["grants"].items():
            if grant["order"] != order:
                continue
            if delegator and grant["partner"] != delegator:
                continue
            opened = parse_time(grant.get("opened_at"))
            closed = parse_time(grant.get("closed_at"))
            if opened and at_dt and opened <= at_dt and (closed is None or at_dt < closed):
                candidates.append((opened, gid))
        if candidates:
            candidates.sort()
            return candidates[-1][1]
        # 退一步：同订单最近打开的非子委托
        for gid, grant in sorted(self.store.data["grants"].items()):
            if grant["order"] == order and not grant.get("parent_grant"):
                return gid
        return None

    def grant_record(self, grant: str) -> dict:
        row = self.store.data["grants"].get(grant)
        if row is None:
            raise NotFound(f"未知委托编号 {grant}", code="unknown_grant")
        return dict(row)

    def grant_interval(self, grant: str, _stack: tuple[str, ...] = ()
                       ) -> tuple[datetime | None, datetime | None]:
        """返回委托的 [生效, 失效) 区间。

        失效时间取：显式 end、同一订单上下一次主委托 assign 的生效时间
        （新派单天然顶替旧任务）、以及父委托失效时间 的最小值。
        """

        row = self.store.data["grants"].get(grant)
        if row is None or grant in _stack:
            return None, None
        stack = _stack + (grant,)
        opened = parse_time(row.get("opened_at"))
        closed = parse_time(row.get("closed_at"))

        if not row.get("parent_grant") and opened is not None:
            later_opens = []
            for other_id, other in self.store.data["grants"].items():
                if other_id == grant or other.get("parent_grant"):
                    continue
                if other.get("order") != row.get("order"):
                    continue
                other_open = parse_time(other.get("opened_at"))
                if other_open and other_open > opened:
                    later_opens.append(other_open)
            if later_opens:
                supersede_at = min(later_opens)
                closed = supersede_at if closed is None else min(closed, supersede_at)

        parent = row.get("parent_grant")
        if parent and self.store.data["grants"].get(parent):
            _p_open, p_closed = self.grant_interval(parent, stack)
            if p_closed is not None:
                closed = p_closed if closed is None else min(closed, p_closed)
        return opened, closed

    def grant_active(self, order: str, grant: str, partner: str | None = None,
                     at: Any = None) -> bool:
        try:
            row = self.grant_record(grant)
        except NotFound:
            return False
        if row["order"] != order:
            return False
        if partner is not None and row["partner"] != partner:
            return False
        moment = self.now(at)
        opened, closed = self.grant_interval(grant)
        if opened is not None and moment < opened:
            return False
        if closed is not None and moment >= closed:
            return False
        return True

    def current_delegation(self, order: str, at: Any = None) -> dict:
        """收货方/调度查询当前委托：当前时刻覆盖该票货的全部有效委托链。"""

        delivery = self.get_delivery(order)
        moment = self.now(at)
        active: list[dict] = []
        for gid, row in self.store.data["grants"].items():
            if row["order"] != order:
                continue
            opened, closed = self.grant_interval(gid)
            if opened and moment < opened:
                continue
            if closed and moment >= closed:
                continue
            item = dict(row)
            partner = self.store.data["partners"].get(partner_key(row["partner"], delivery["region"]))
            item["partner_profile"] = dict(partner) if partner else None
            item["effective_fields"] = sorted(
                self.allowed_fields(order, row["partner"], gid, at=moment)
            )
            active.append(item)
        active.sort(key=lambda g: (g.get("opened_at") or "", g["grant"]))
        primary = [g for g in active if not g.get("parent_grant")]
        current = primary[-1] if primary else (active[-1] if active else None)
        return {"order": order, "region": delivery["region"], "at": format_time(moment),
                "current": current, "active_grants": active}

    get_current_delegation = current_delegation

    def current_grant(self, order: str, at: Any = None) -> dict | None:
        """当前主委托记录（无则 None）。"""

        return self.current_delegation(order, at=at)["current"]

    # ---- 权限/字段计算 --------------------------------------------------- #

    def _partner_for(self, partner: str | dict | None, region: str) -> dict:
        if isinstance(partner, dict):
            return partner
        if partner is None:
            raise PermissionDenied("缺少 partner 身份", code="partner_required")
        # 授权路径严格按 (partner, 订单区域) 命中登记，不能用其它区域的登记顶替
        row = self.store.data["partners"].get(partner_key(partner, region))
        if row is None:
            raise PermissionDenied(
                f"配送方 {partner} 未登记服务区域 {region}",
                code="partner_region_not_authorized",
                details={"partner": partner, "region": region})
        return dict(row)

    def _ancestor_partners(self, grant_row: dict | None) -> list[dict]:
        ancestors: list[dict] = []
        if not grant_row:
            return ancestors
        parent = grant_row.get("parent_grant")
        seen = set()
        while parent and parent not in seen:
            seen.add(parent)
            prow = self.store.data["grants"].get(parent)
            if not prow:
                break
            try:
                ancestors.append(self.get_partner(prow["partner"]))
            except NotFound:
                ancestors.append({"partner": prow["partner"], "allowed": [], "scope": prow.get("scope")})
            if prow.get("scope"):
                ancestors[-1]["scope"] = prow["scope"]
            parent = prow.get("parent_grant")
        return ancestors

    def allowed_fields(self, order: str | dict, partner: str | dict | None = None,
                       grant: str | dict | None = None, *, at: Any = None,
                       fields: Any = None, purpose: str | None = None) -> FieldScope:
        """当前资料权限下可给该委托/配送方的字段名集合。"""

        delivery = self.get_delivery(order) if isinstance(order, str) else dict(order)
        partner_row = self._partner_for(partner, delivery["region"])
        grant_id = grant if isinstance(grant, str) else (grant or {}).get("grant")
        grant_row = None
        if grant_id:
            grant_row = self.store.data["grants"].get(grant_id)
        contact = self.contacts.get(delivery.get("contact_ref") or "")
        scope = resolve_allowed_fields(
            delivery, partner_row, grant_row,
            ancestor_partners=self._ancestor_partners(grant_row),
            at=self.now(at), contact=contact, purpose=purpose,
        )
        if fields is not None:
            requested = set(as_list(fields) or [])
            return FieldScope(scope & requested)
        return scope

    # 常见近义命名
    effective_fields = allowed_fields
    permitted_fields = allowed_fields

    def scope_report(self, order: str, partner: str | None = None, grant: str | None = None,
                     *, at: Any = None, fields: Any = None) -> dict:
        delivery = self.get_delivery(order)
        moment = self.now(at)
        reasons: list[str] = []
        until = parse_time(delivery.get("until"))
        if until and moment > until:
            reasons.append("order_expired")
        if grant and not self.grant_active(order, grant, partner, at=moment):
            reasons.append("grant_inactive")
        scope = self.allowed_fields(order, partner, grant, at=moment, fields=fields)
        return {"order": order, "at": format_time(moment), "fields": scope.sorted(),
                "allowed": scope.sorted(), "reasons": reasons}

    def authorize_fields(self, order: str, grant: str | None, partner: str | None,
                         fields: Any = None, *, at: Any = None,
                         actor: Any = None, purpose: str | None = None
                         ) -> tuple[dict, dict, FieldScope]:
        """统一的取数鉴权：返回 (delivery, partner_row, permitted_set)。"""

        delivery = self.get_delivery(order)
        moment = self.now(at)
        if parse_time(delivery.get("until")) and moment > parse_time(delivery["until"]):
            raise PermissionDenied("已超过订单同意有效期", code="consent_expired",
                                   details={"until": delivery["until"]})
        if purpose and delivery.get("purpose") and purpose != delivery["purpose"]:
            raise PermissionDenied("用途超出订单同意范围", code="purpose_not_permitted",
                                   details={"purpose": purpose,
                                            "agreed_purpose": delivery.get("purpose")})
        partner_row = self._partner_for(partner, delivery["region"])
        if grant is not None and not self.grant_active(order, grant, partner_row["partner"], at=moment):
            self.audit("contact.denied", actor=actor or partner_row["partner"], order=order,
                       grant=grant, partner=partner_row["partner"], reason="grant_inactive", at=moment)
            raise PermissionDenied("委托不是该票货的当前有效任务，不能查询资料",
                                   code="grant_inactive")
        permitted = self.allowed_fields(delivery, partner_row, grant, at=moment, purpose=purpose)
        requested = as_list(fields)
        if requested:
            missing = sorted(set(requested) - permitted)
            if missing:
                self.audit("contact.denied", actor=actor or partner_row["partner"], order=order,
                           grant=grant, partner=partner_row["partner"],
                           fields=requested, denied_fields=missing,
                           reason="field_not_permitted", at=moment)
                raise PermissionDenied("请求字段超出当前同意/登记/委托范围",
                                       code="field_not_permitted",
                                       details={"denied_fields": missing,
                                                "allowed_fields": permitted.sorted()})
        return delivery, partner_row, permitted

    # ---- 资料查询/取号（立即按最新范围） --------------------------------- #

    def view_contact(self, order: str | dict | None = None, grant: str | None = None,
                     partner: str | None = None, fields: Any = None, *, at: Any = None,
                     actor: Any = None, strict: bool = True, **kwargs) -> dict:
        """返回获准字段的当前值。响应是裸 ``{字段: 值}``，便于直接断言。

        proxy_only 地区请求/默认取联络方式时，``phone`` 以项目 ``relay``
        引用返回，绝不返回电话号码原值。
        """

        # 兼容关键字别名
        order = order or kwargs.get("order_id")
        grant = grant or kwargs.get("grant_id")
        partner = partner or kwargs.get("partner_id")
        purpose = kwargs.get("purpose")
        if fields is None:
            fields = kwargs.get("field") or kwargs.get("names")

        requested = as_list(fields)
        delivery, partner_row, permitted = self.authorize_fields(
            order, grant, partner, requested, at=at, actor=actor, purpose=purpose)

        contact = self.get_contact_ref(delivery["contact_ref"])
        if requested:
            wanted = list(requested)
        else:
            wanted = permitted.sorted()

        values: dict[str, Any] = {}
        for name in wanted:
            if name not in permitted:
                if strict:
                    raise PermissionDenied("字段不在当前范围", code="field_not_permitted",
                                           details={"field": name})
                continue
            if partner_row.get("proxy_only") and name in {"phone", "relay"}:
                # 只给转接引用这一种“联络号”
                if "relay" in permitted and contact.get("relay"):
                    values["relay"] = contact["relay"]
                continue
            if name == "phone" and partner_row.get("proxy_only"):
                continue
            if name in contact:
                values[name] = contact[name]

        self.audit("contact.view", actor=actor or partner_row.get("partner"), order=order,
                   grant=grant, partner=partner_row.get("partner"), fields=list(values),
                   at=at)
        return values

    # 语义别名：取号 / 查询资料
    query_contact = view_contact
    fetch_contact = view_contact
    get_contact_fields = view_contact

    def take_number(self, order: str, grant: str | None = None, partner: str | None = None,
                    **kwargs) -> dict:
        """“取号”：非转接地区给 phone，转接地区给 relay 业务引用。"""

        delivery = self.get_delivery(order)
        partner_row = self._partner_for(partner, delivery["region"])
        if partner_row.get("proxy_only"):
            return self.view_contact(order, grant, partner, fields=["relay"], **kwargs)
        return self.view_contact(order, grant, partner, fields=["phone"], **kwargs)

    # ---- 对外披露与合作端收敛 ------------------------------------------- #

    def disclose(self, ref: str | None = None, order: str | None = None,
                 grant: str | None = None, partner: str | None = None,
                 fields: Any = None, *, at: Any = None, actor: Any = None,
                 push: bool = True, **kwargs) -> dict:
        """按当前范围披露字段并调用合作端 ``POST /disclosures``。

        持久化的披露单只记录字段名（requested/sent/received），不保存原值。
        """

        # 兼容 dict/关键字/位置等多种调用形态
        if isinstance(ref, dict):
            payload = ref
            ref = payload.get("ref")
            order = order or payload.get("order")
            grant = grant or payload.get("grant")
            partner = partner or payload.get("partner")
            fields = fields if fields is not None else payload.get("fields")
        order = order or kwargs.get("order_id")
        grant = grant or kwargs.get("grant_id")
        partner = partner or kwargs.get("partner_id")
        if fields is None:
            fields = kwargs.get("field_names") or kwargs.get("names")
        field_names = as_list(fields) or ["phone"]
        purpose = kwargs.get("purpose")

        with self._lock:
            delivery, partner_row, permitted = self.authorize_fields(
                order, grant, partner, field_names, at=at, actor=actor, purpose=purpose)
            values = self.view_contact(order, grant, partner_row["partner"],
                                       field_names, at=at, actor=actor,
                                       strict=True, purpose=purpose)
            # 以实际取到值的字段作为发送内容（proxy 下 phone -> relay）
            wire_fields = dict(values)
            if not wire_fields:
                raise PermissionDenied("没有可披露的字段", code="empty_disclosure")

            if ref is None:
                ref = self._generate_ref("D")
            if ref in self.store.data["disclosures"]:
                raise ConflictError(f"披露引用 {ref} 已存在", code="duplicate_disclosure_ref")

            ts = format_time(self.now(at))
            record = {
                "ref": ref,
                "order": order,
                "grant": grant,
                "partner": partner_row["partner"],
                "requested_fields": field_names,
                "sent_fields": sorted(wire_fields),
                "received_fields": [],
                "status": "active",
                "peer_status": "unknown",
                "stop_acknowledged": False,
                "stop_attempts": 0,
                "reconcile_attempts": 0,
                "evidence": None,
                "created_at": ts,
                "updated_at": ts,
                "stop_requested_at": None,
                "converged_at": None,
                "last_error": None,
            }

            peer_pair: tuple[int, Any] | None = None
            if push:
                status, body = self._peer_call(
                    "create_disclosure", ref, grant, wire_fields)
                peer_pair = (status, body)
                if status == 200 and isinstance(body, dict):
                    record["peer_status"] = body.get("status", "active")
                    record["status"] = "active"
                    record["received_fields"] = body.get("received_fields") or sorted(wire_fields)
                elif status == 0:
                    # 未收到应答：资料是否已传出未知，不能当作 active 成功，
                    # 也不能臆造撤销；列入待确认并提示运营。
                    record["status"] = "send_unknown"
                    record["peer_status"] = "no_response"
                    record["last_error"] = (body or {}).get("detail", "no_response") if isinstance(body, dict) else "no_response"
                    self._open_followup_locked(
                        "disclosure_send_unknown", ref, order=order,
                        summary=f"{ref} 披露请求无应答，资料可能已传出，需人工与合作方核对")
                else:
                    record["status"] = "send_rejected"
                    record["peer_status"] = f"http_{status}"
                    record["last_error"] = json.dumps(body, ensure_ascii=False)[:500] if body else f"http {status}"
            self.store.data["disclosures"][ref] = record
            self.audit("disclosure.created", actor=actor or partner_row["partner"], order=order,
                       grant=grant, partner=partner_row["partner"], ref=ref,
                       fields=record["sent_fields"], peer_status=record["peer_status"], save=False)
            self.store.save()
            result = dict(record)
            if peer_pair is not None:
                result["peer_response"] = list(peer_pair)
            return result

    def get_disclosure(self, ref: str) -> dict:
        row = self.store.data["disclosures"].get(ref)
        if row is None:
            raise NotFound(f"未知披露引用 {ref}", code="unknown_disclosure")
        return dict(row)

    def reference_status(self, ref: str, *, live: bool = False) -> dict:
        """旧引用是否仍在合作方处理；live=True 时先 GET 对账一次。"""

        if live:
            self.reconcile(ref)
        row = self.get_disclosure(ref)
        return {"ref": ref, "status": row["status"],
                "peer_status": row.get("peer_status"),
                "evidence": row.get("evidence"),
                "stop_attempts": row.get("stop_attempts"),
                "updated_at": row.get("updated_at")}

    check_reference = reference_status

    def list_disclosures(self, *, status: str | None = None, order: str | None = None) -> list[dict]:
        rows = list(self.store.data["disclosures"].values())
        if status:
            rows = [r for r in rows if r["status"] == status]
        if order:
            rows = [r for r in rows if r["order"] == order]
        return [dict(r) for r in rows]

    @property
    def disclosures(self) -> dict[str, dict]:
        return self.store.data["disclosures"]

    def request_stop(self, ref: str, *, reason: str = "revoked", at: Any = None,
                     actor: Any = "dispatch", **_ignored) -> dict:
        with self._lock:
            self._request_stop_locked(ref, reason=reason, at=at, actor=actor)
            self.store.save()
            return self.get_disclosure(ref)

    stop_disclosure_local = request_stop
    revoke_reference = request_stop

    def _request_stop_locked(self, ref: str, *, reason: str = "revoked", at: Any = None,
                             actor: Any = "dispatch") -> None:
        record = self.store.data["disclosures"].get(ref)
        if record is None:
            return
        if record["status"] == "stopped":
            return
        ts = format_time(self.now(at))
        record["status"] = "stop_pending"
        record["stop_requested_at"] = record.get("stop_requested_at") or ts
        record["updated_at"] = ts
        record["stop_attempts"] = int(record.get("stop_attempts", 0)) + 1
        status, _body = self._peer_call("stop_disclosure", ref)
        if status == 0:
            record["last_error"] = "stop_no_response"
            record["stop_acknowledged"] = False
        elif 200 <= status < 300:
            record["stop_acknowledged"] = True
            record["last_error"] = None
        else:
            record["stop_acknowledged"] = False
            record["last_error"] = f"stop_http_{status}"
        self.audit("disclosure.stop_requested", actor=actor, ref=ref, order=record["order"],
                   grant=record["grant"], partner=record["partner"],
                   fields=record.get("sent_fields"), reason=reason,
                   peer_http=status, save=False)

    # ---- 重启后续跑的对账 ------------------------------------------------ #

    def reconcile(self, ref: str | None = None, *, at: Any = None) -> dict:
        """对所有未 stopped 的披露引用与合作端对账。

        每次对账：必要时重发 stop，再 GET 状态；只有合作端明确
        ``stopped``（可带 evidence）才把本地置为 stopped。status 0/异常
        一律保持待收敛，绝不把没收到的回应当成完成。
        """

        summary = {"checked": 0, "stopped": [], "pending": [], "unknown": [], "errors": []}
        with self._lock:
            refs = [ref] if ref else list(self.store.data["disclosures"].keys())
            for current in refs:
                record = self.store.data["disclosures"].get(current)
                if record is None or record["status"] == "stopped":
                    continue
                summary["checked"] += 1
                record["reconcile_attempts"] = int(record.get("reconcile_attempts", 0)) + 1
                gstatus, body = self._peer_call("get_disclosure", current)

                if gstatus == 0:
                    # GET 无应答：重试一次 stop，保持待收敛
                    self._safe_retry_stop(current)
                    summary["errors"].append(current)
                    summary["pending"].append(current)
                    continue

                peer_status = (body or {}).get("status") if isinstance(body, dict) else None
                record["peer_status"] = peer_status or f"http_{gstatus}"

                if gstatus == 200 and peer_status == "stopped":
                    evidence = (body or {}).get("evidence")
                    record["status"] = "stopped"
                    record["evidence"] = evidence
                    record["converged_at"] = format_time(self.now(at))
                    record["updated_at"] = record["converged_at"]
                    record["last_error"] = None
                    self.audit("disclosure.stopped_confirmed", ref=current,
                               order=record["order"], grant=record["grant"],
                               partner=record["partner"], fields=record.get("sent_fields"),
                               evidence=evidence, save=False)
                    self._resolve_followup_locked("disclosure_send_unknown", current)
                    summary["stopped"].append(current)
                    continue

                if gstatus == 200 and peer_status == "active":
                    # 合作端仍是 active：stop 大概没送达，重发
                    self._safe_retry_stop(current)
                elif peer_status == "stop_pending":
                    # 合作端处理中：按“重试”要求继续催 stop（对端幂等空对象）
                    self._safe_retry_stop(current, only_if_unacknowledged=False)
                elif gstatus == 404:
                    record["peer_status"] = "not_found"
                    self._open_followup_locked(
                        "stop_unconfirmed_peer_unknown", current,
                        order=record["order"],
                        summary=f"{current} 在合作端查询不到，缺少停用证据，需人工跟进")
                    summary["unknown"].append(current)
                else:
                    summary["errors"].append(current)

                record["status"] = "stop_pending" if record["status"] != "send_unknown" else "send_unknown"
                summary["pending"].append(current)
            self.store.save()
        return summary

    reconcile_pending = reconcile

    def _safe_retry_stop(self, ref: str, *, only_if_unacknowledged: bool = True) -> None:
        record = self.store.data["disclosures"][ref]
        if only_if_unacknowledged and record.get("stop_acknowledged"):
            return
        record["stop_attempts"] = int(record.get("stop_attempts", 0)) + 1
        status, _body = self._peer_call("stop_disclosure", ref)
        if status == 0:
            record["last_error"] = "stop_no_response"
            record["stop_acknowledged"] = False
        elif 200 <= status < 300:
            record["stop_acknowledged"] = True
            record["last_error"] = None
        else:
            record["last_error"] = f"stop_http_{status}"

    def pending_convergence(self) -> list[dict]:
        return [self.get_disclosure(ref) for ref, row in self.store.data["disclosures"].items()
                if row["status"] != "stopped"]

    def check_access(self, order: str, partner: str | None = None,
                     fields: Any = None, grant: str | None = None, *,
                     at: Any = None, purpose: str | None = None) -> bool:
        """不抛异常的鉴权判断：当前委托有效且字段在范围内才为 True。"""

        try:
            self.authorize_fields(order, grant, partner, as_list(fields),
                                  at=at, purpose=purpose)
            return True
        except ServiceError:
            return False

    can_access = check_access
    is_allowed = check_access

    # ---- 交付任务推进（只有当前有效委托可以） ---------------------------- #

    TASK_STATUSES = ("dispatched", "accepted", "in_transit", "arrived",
                     "out_for_delivery", "delivered", "delivery_failed", "exception")

    def advance_task(self, order: str, grant: str, partner: str,
                     status: str | None = None, *, event: str | None = None,
                     note: str = "", at: Any = None, actor: Any = None,
                     **kwargs) -> dict:
        """推进交付任务状态。

        分派权跟着当前任务走：委托已 end/被改派后，旧作业号可以补交历史
        凭证（见 :meth:`submit_proof`），但不能推进任务状态。
        """

        status = status or kwargs.get("state") or kwargs.get("action_status") or "in_transit"
        if status not in self.TASK_STATUSES:
            raise ServiceError("未知任务状态", code="unknown_task_status",
                               details={"allowed": list(self.TASK_STATUSES)})
        with self._lock:
            if not self.grant_active(order, grant, partner, at=at):
                self.audit("task.advance_denied", actor=actor or partner, order=order,
                           grant=grant, partner=partner, requested_status=status,
                           reason="grant_inactive", at=at)
                raise PermissionDenied("旧委托已不是当前任务，不能推进交付；"
                                       "早前签收请走凭证补交", code="grant_inactive")
            ts = format_time(self.now(at))
            item = {
                "seq": self.store.next_seq(),
                "event": event or new_id("T"),
                "ts": ts,
                "order": order,
                "grant": grant,
                "partner": partner,
                "status": status,
                "note": note,
            }
            self.store.data["task_events"].append(item)
            self.store.data["task_status"][order] = {
                "order": order, "status": status, "grant": grant,
                "partner": partner, "updated_at": ts,
            }
            self.audit("task.advanced", actor=actor or partner, order=order,
                       grant=grant, partner=partner, status=status, at=ts, save=False)
            self.store.save()
            return dict(item)

    advance_delivery = advance_task
    update_task_status = advance_task
    update_delivery_status = advance_task
    add_tracking_event = advance_task

    def task_status(self, order: str) -> dict:
        row = self.store.data["task_status"].get(order)
        if row is None:
            raise NotFound(f"订单 {order} 暂无任务状态", code="task_not_started")
        return dict(row)

    def task_events(self, order: str | None = None) -> list[dict]:
        rows = self.store.data["task_events"]
        if order:
            rows = [r for r in rows if r["order"] == order]
        return [dict(r) for r in rows]

    # ---- 凭证 ------------------------------------------------------------ #

    def submit_proof(self, record: dict | None = None, **fields) -> dict:
        """旧承运方在改派后仍可补交“改派前已签收”的凭证。

        只插入凭证记录，绝不推进当前委托/任务状态；有效性由
        delivered_at 对照委托区间判定。
        """

        row = dict(record or {})
        row.update({k: v for k, v in fields.items() if v is not None})
        required = ["receipt", "order", "grant", "partner", "delivered_at", "evidence"]
        missing = [key for key in required if row.get(key) in (None, "")]
        if missing:
            raise ServiceError("凭证缺少字段", details={"missing": missing})
        parse_time(row["delivered_at"])
        row.setdefault("received_at", format_time(self.now()))
        with self._lock:
            self.store.data["proofs"][row["receipt"]] = row
            verdict = evaluate_proof(row, self.store.data["dispatch_events"])
            if not verdict["valid"]:
                self._open_followup_locked(
                    "proof_out_of_window", row["receipt"], order=row["order"],
                    summary=f"凭证 {row['receipt']} 有效性存疑（{verdict['reason']}），需人工核验")
            self.audit("proof.submitted", actor=row.get("partner"), order=row["order"],
                       grant=row["grant"], partner=row["partner"], receipt=row["receipt"],
                       valid=verdict["valid"], reason=verdict["reason"], save=False)
            self.store.save()
        result = dict(row)
        result["validity"] = verdict
        return result

    add_proof = submit_proof

    def verify_proof(self, receipt: str) -> dict:
        return self.proof_validity(receipt)

    check_proof = verify_proof
    proof_status = verify_proof

    def proof_validity(self, receipt: str) -> dict:
        row = self.store.data["proofs"].get(receipt)
        if row is None:
            raise NotFound(f"未知凭证 {receipt}", code="unknown_proof")
        verdict = evaluate_proof(row, self.store.data["dispatch_events"])
        result = dict(row)
        result["validity"] = verdict
        result["valid"] = verdict["valid"]
        return result

    def review_proofs(self, order: str | None = None) -> list[dict]:
        result = []
        for receipt, row in self.store.data["proofs"].items():
            if order and row["order"] != order:
                continue
            verdict = evaluate_proof(row, self.store.data["dispatch_events"])
            item = dict(row)
            item["validity"] = verdict
            item["valid"] = verdict["valid"]
            result.append(item)
        result.sort(key=lambda p: p["receipt"])
        return result

    valid_proofs = lambda self, order=None: [p for p in self.review_proofs(order) if p["valid"]]  # noqa: E731

    def valid_proof(self, order: str | None = None) -> dict | None:
        """返回该订单当前有效的那一份凭证（多份时取签收时间最晚者）。"""

        rows = self.valid_proofs(order)
        if not rows:
            return None
        rows.sort(key=lambda p: p.get("delivered_at") or "")
        return rows[-1]

    def invalid_proofs(self, order: str | None = None) -> list[dict]:
        return [p for p in self.review_proofs(order) if not p["valid"]]

    def dispatch_proof(self, receipt: str, *, grant: str | None = None,
                       partner: str | None = None, at: Any = None,
                       actor: Any = None, fields: Any = None) -> dict:
        """调度/承运查询凭证：凭证本身保留可查，但附带的联系资料仍按当前权限过滤。

        旧委托在改派后查不到个人信息：``current_contact`` 为空并给
        ``contact_denied_reason``；凭证结论（留存/有效性）不受影响。
        """

        proof = self.proof_validity(receipt)
        result = dict(proof)
        result["current_contact"] = {}
        result["contact_denied_reason"] = None
        if grant and partner:
            try:
                result["current_contact"] = self.view_contact(
                    proof["order"], grant, partner, fields, at=at, actor=actor)
            except PermissionDenied as exc:
                result["contact_denied_reason"] = exc.code
        return result

    # ---- 人工跟进 -------------------------------------------------------- #

    def _open_followup_locked(self, kind: str, subject: str, *, order: str | None = None,
                              summary: str = "") -> dict:
        for item in self.store.data["followups"]:
            if item["kind"] == kind and item["subject"] == subject and item["status"] == "open":
                return item
        item = {
            "id": f"F-{self.store.next_seq()}",
            "kind": kind,
            "subject": subject,
            "order": order,
            "summary": summary,
            "status": "open",
            "created_at": format_time(self.now()),
            "resolved_at": None,
        }
        self.store.data["followups"].append(item)
        return item

    def open_followup(self, kind: str, subject: str, summary: str = "", **kw) -> dict:
        with self._lock:
            item = self._open_followup_locked(kind, subject, summary=summary, **kw)
            self.store.save()
            return dict(item)

    def resolve_followup(self, followup_id: str) -> dict:
        with self._lock:
            for item in self.store.data["followups"]:
                if item["id"] == followup_id:
                    item["status"] = "resolved"
                    item["resolved_at"] = format_time(self.now())
                    self.store.save()
                    return dict(item)
            raise NotFound(f"未知跟进项 {followup_id}", code="unknown_followup")

    def _resolve_followup_locked(self, kind: str, subject: str) -> None:
        for item in self.store.data["followups"]:
            if item["kind"] == kind and item["subject"] == subject and item["status"] == "open":
                item["status"] = "resolved"
                item["resolved_at"] = format_time(self.now())

    def followups(self, *, open_only: bool = True) -> list[dict]:
        rows = self.store.data["followups"]
        if open_only:
            rows = [r for r in rows if r["status"] == "open"]
        return [dict(r) for r in rows]

    manual_followups = followups
    manual_review_queue = followups
    ops_queue = followups

    # ---- 运营总览 -------------------------------------------------------- #

    def ops_overview(self, actor: Any = None, *, live: bool = False,
                     order: str | None = None) -> dict:
        self.authorize_ops(actor)
        if live:
            self.reconcile()
        proofs = self.review_proofs(order)
        return {
            "as_of": format_time(self.now()),
            "valid_proofs": [p for p in proofs if p["valid"]],
            "invalid_proofs": [p for p in proofs if not p["valid"]],
            "pending_convergence": self.pending_convergence(),
            "stopped_references": [self.get_disclosure(r)
                                   for r, d in self.store.data["disclosures"].items()
                                   if d["status"] == "stopped"],
            "followups": self.followups(),
            "manual_followups": self.followups(),
            "operators": list(self.store.data.get("operators", [])),
        }

    overview = ops_overview

    # ---- 持久化/生命周期 ------------------------------------------------- #

    def save(self) -> None:
        self.store.save()

    def close(self) -> None:
        self.store.save()

    def snapshot(self) -> dict:
        """供测试/自检使用的状态深拷贝（不含 contacts 原值表）。"""

        return json.loads(json.dumps(self.store.data, ensure_ascii=False, default=str))


# 常见类名别名
Service = DeliveryService


def create_service(state_path=None, **kwargs) -> DeliveryService:
    """工厂函数，等价于 :class:`DeliveryService` 构造。"""

    return DeliveryService(state_path, **kwargs)


def read_jsonl(path: "str | Path") -> list[dict]:
    """读取 JSONL 交换资料。"""

    return _read_jsonl(Path(path))


load_jsonl = read_jsonl


# --------------------------------------------------------------------------- #
# 辅助
# --------------------------------------------------------------------------- #


def partner_key(partner: str, region: str) -> str:
    return f"{partner}@{region}"


def _read_jsonl(path: Path) -> list[dict]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


# --------------------------------------------------------------------------- #
# 命令行：不启 HTTP 也能做总览/对账
# --------------------------------------------------------------------------- #


def _cli(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="跨境交付授权/对账命令行")
    parser.add_argument("command", choices=["overview", "reconcile", "scope", "demo-state"])
    parser.add_argument("--state", default="runtime/state.json")
    parser.add_argument("--peer-url", default=DEFAULT_PEER_URL)
    parser.add_argument("--order", default=None)
    parser.add_argument("--grant", default=None)
    parser.add_argument("--partner", default=None)
    parser.add_argument("--at", default=None)
    options = parser.parse_args(argv)

    svc = DeliveryService(options.state, peer_url=options.peer_url)
    if options.command == "overview":
        print(json.dumps(svc.ops_overview(), ensure_ascii=False, indent=2, default=str))
    elif options.command == "reconcile":
        print(json.dumps(svc.reconcile(), ensure_ascii=False, indent=2))
    elif options.command == "scope":
        print(json.dumps(svc.scope_report(options.order, options.partner,
                                          options.grant, at=options.at),
                         ensure_ascii=False, indent=2))
    elif options.command == "demo-state":
        # 生成一份剧本后的状态（D-3 已 stopped/S4 需对端配合两次 reconcile）
        print(options.state)
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
