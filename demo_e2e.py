#!/usr/bin/env python3
"""按《交换资料约定》跑通完整业务剧本，并与 samples/peer.json 本地对端联调。

剧本覆盖：
1. 字段范围矩阵（订单同意 × 配送方登记 × 委托链 × proxy_only）；
2. 同一签收人两票订单的同意互不打通；
3. D-3/G-3/phone 精确报文 → 对端 active；
4. 改派后旧委托不能再查资料；stop 遇到断连不得显示完成；
5. “重启”服务（同状态文件新建实例）后继续对账：stop_pending → stopped/S4；
6. 流水/状态文件不含电话原值；运营视图给出有效凭证与人工跟进项。

用法：
    python3 demo_e2e.py                 # 自动起一个临时报文重放对端
    python3 demo_e2e.py --peer-url http://127.0.0.1:8087
    python3 demo_e2e.py --state runtime/demo.json
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from threading import Thread

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from peer_replay import serve_recording  # noqa: E402
from service import DeliveryService, PermissionDenied  # noqa: E402

RECORDING = ROOT / "samples" / "peer.json"
T0 = "2026-11-27T00:45:00Z"
T1 = "2026-11-27T01:30:00Z"
TPHONE = "2026-11-27T01:45:00Z"
TAFTER = "2026-11-27T02:05:00Z"

CHECKS: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    CHECKS.append((name, bool(condition), detail))
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {name}" + (f" — {detail}" if detail else ""))


def expect_denied(name: str, fn) -> None:
    try:
        fn()
    except PermissionDenied as exc:
        check(name, True, exc.code)
        return
    check(name, False, "应当拒绝却放行")


def run(peer_url: str, state_path: str) -> int:
    svc = DeliveryService(state_path, peer_url=peer_url)

    print("\n== 1. 数据范围与逐票同意 ==")
    check("X3/B1/G-3 可见 phone+drop_zone",
          svc.allowed_fields("X3", "B1", "G-3", at=T1) == {"phone", "drop_zone"})
    check("X3/B1a/G-3A 协力方只见 drop_zone（不大于委托方）",
          svc.allowed_fields("X3", "B1a", "G-3A", at=T1) == {"drop_zone"})
    check("X3/B2/G-4 登记范围无 phone",
          svc.allowed_fields("X3", "B2", "G-4", at=TAFTER) == {"drop_zone"})
    check("X4 与 X3 同一收车人，但 X4 未同意 phone",
          svc.allowed_fields("X4", "B1") == {"drop_zone"})
    expect_denied("X4 借同一 C-3 资料取 phone 被拒",
                  lambda: svc.view_contact("X4", None, "B1", ["phone"]))
    check("X5/B3 proxy_only 只给 relay 引用",
          svc.view_contact("X5", None, "B3") == {"relay": "relay-C8"})
    check("proxy 响应不含电话原值",
          "+1-202-555-0108" not in json.dumps(svc.view_contact("X5", None, "B3")))

    print("\n== 2. 改派与旧委托 ==")
    expect_denied("02:00 后旧承运方 B1/G-3 再查电话被拒",
                  lambda: svc.view_contact("X3", "G-3", "B1", ["phone"], at=TAFTER))
    expect_denied("旧协力方 B1a/G-3A 同样失效",
                  lambda: svc.view_contact("X3", "G-3A", "B1a", at=TAFTER))
    check("当前任务已切到 G-4/B2",
          svc.current_delegation("X3", at=TAFTER)["current"]["grant"] == "G-4")
    p3 = svc.proof_validity("P3")
    p4 = svc.proof_validity("P4")
    check("P3 签收于 01:50（改派前）有效，尽管 02:28 才上传", p3["valid"] is True)
    check("P4 签收于 02:09（G-3 结束后）无效",
          p4["valid"] is False and p4["validity"]["reason"] == "after_end")

    print("\n== 3. 与合作端联调 D-3 ==")
    disc = svc.disclose("D-3", order="X3", grant="G-3", partner="B1",
                        fields=["phone"], at=TPHONE)
    check("POST /disclosures 精确契约被重放端确认 active + received_fields=[phone]",
          disc["status"] == "active" and disc["received_fields"] == ["phone"],
          str(disc.get("peer_response")))

    stop = svc.request_stop("D-3", at=TAFTER)
    check("stop 连接断开(status 0)后本地只是 stop_pending，不显示完成",
          stop["status"] == "stop_pending" and not stop["stop_acknowledged"])

    print("\n== 4. 重启后续跑对账 ==")
    svc.close()
    svc2 = DeliveryService(state_path, peer_url=peer_url)
    check("重启后仍记得 D-3 待收敛",
          svc2.get_disclosure("D-3")["status"] == "stop_pending")
    r1 = svc2.reconcile()
    check("第一次 GET 对端报 stop_pending，仍不完成",
          r1["stopped"] == [] and svc2.get_disclosure("D-3")["peer_status"] == "stop_pending")
    r2 = svc2.reconcile()
    final = svc2.get_disclosure("D-3")
    check("第二次 GET 对端报 stopped + 证据 S4，才标记完成",
          r2["stopped"] == ["D-3"] and final["status"] == "stopped"
          and final["evidence"] == "S4")

    print("\n== 5. 隐私与运营视图 ==")
    state_blob = Path(state_path).read_text(encoding="utf-8")
    audit_blob = json.dumps(svc2.audit_entries(), ensure_ascii=False)
    check("状态文件不保留电话原值", "+1-202-555-0103" not in state_blob)
    check("流水不保留电话原值，但保留字段名 phone",
          "+1-202-555-0103" not in audit_blob and '"phone"' in audit_blob)
    overview = svc2.ops_overview(actor={"id": "ops", "roles": ["ops"]})
    check("运营可查有效凭证仅 P3",
          [p["receipt"] for p in overview["valid_proofs"]] == ["P3"])
    check("运营可见 D-3 的合作方停用证据 S4",
          any(d["ref"] == "D-3" and d["evidence"] == "S4"
              for d in overview["stopped_references"]))

    # P4 这类越窗凭证在“复核”后形成人工跟进项（提交/复核动作触发）
    svc2.submit_proof(receipt="P4", order="X3", grant="G-3", partner="B1",
                      delivered_at="2026-11-27T02:09:00Z", evidence="e-4",
                      received_at="2026-11-27T02:33:00Z")
    followups = [(f["kind"], f["subject"]) for f in svc2.followups()]
    check("P4 进入人工跟进队列", ("proof_out_of_window", "P4") in followups)

    failed = [c for c in CHECKS if not c[1]]
    print(f"\n共 {len(CHECKS)} 项，通过 {len(CHECKS) - len(failed)}，失败 {len(failed)}")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="交付服务端到端联调剧本")
    parser.add_argument("--peer-url", default=None, help="默认自动起临时报文重放端")
    parser.add_argument("--state", default=None, help="状态 JSON 路径，默认临时文件")
    parser.add_argument("--recording", default=str(RECORDING))
    options = parser.parse_args()

    state_path = options.state
    if state_path is None:
        state_path = str(Path(tempfile.mkdtemp(prefix="delivery-demo-")) / "state.json")
    Path(state_path).parent.mkdir(parents=True, exist_ok=True)

    if options.peer_url:
        return run(options.peer_url, state_path)

    server = serve_recording(options.recording, 0)
    port = server.server_port
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    thread.start()
    try:
        return run(f"http://127.0.0.1:{port}", state_path)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


if __name__ == "__main__":
    raise SystemExit(main())
