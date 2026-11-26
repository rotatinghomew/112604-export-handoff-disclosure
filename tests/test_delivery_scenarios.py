"""交付服务的端到端规则与对账场景（标准库 unittest，全部本地、无外部依赖）。"""

import http.client
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from threading import Thread

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from peer_replay import serve_recording  # noqa: E402
from service import DeliveryService, PermissionDenied  # noqa: E402

RECORDING = _ROOT / "samples" / "peer.json"

T_BEFORE = "2026-11-27T01:00:00Z"
T_MID = "2026-11-27T01:30:00Z"
T_PHONE = "2026-11-27T01:45:00Z"
T_AFTER = "2026-11-27T02:05:00Z"
T_LATE = "2026-11-29T00:00:00Z"


class ScopeRulesTest(unittest.TestCase):
    def setUp(self):
        self.svc = DeliveryService()

    def test_order_consent_intersects_partner_registration(self):
        # X3 同意 phone+drop_zone；B1 获准 phone/drop_zone/relay
        self.assertEqual(self.svc.allowed_fields("X3", "B1", "G-3", at=T_BEFORE),
                         {"phone", "drop_zone"})
        # B2 只获准 drop_zone/relay，没有 phone
        self.assertEqual(self.svc.allowed_fields("X3", "B2", "G-4", at=T_AFTER),
                         {"drop_zone"})

    def test_subdelegate_scope_is_same_or_smaller(self):
        # B1a 是 B1 的登记协力方，自身只获准 drop_zone
        scope = self.svc.allowed_fields("X3", "B1a", "G-3A", at=T_MID)
        self.assertEqual(scope, {"drop_zone"})
        values = self.svc.view_contact("X3", "G-3A", "B1a", at=T_MID)
        self.assertEqual(values, {"drop_zone": "E-row7"})
        with self.assertRaises(PermissionDenied):
            self.svc.view_contact("X3", "G-3A", "B1a", ["phone"], at=T_MID)

    def test_consent_is_per_order_even_for_same_person(self):
        # X4 与 X3 同为 R7 / C-3，但 X4 的 choice 只有 drop_zone
        self.assertEqual(self.svc.allowed_fields("X4", "B1"), {"drop_zone"})
        with self.assertRaises(PermissionDenied):
            self.svc.view_contact("X4", None, "B1", ["phone"])
        self.assertEqual(self.svc.view_contact("X4", None, "B1"),
                         {"drop_zone": "E-row7"})
        # 同一联络资料在 X3 上仍可按 X3 同意取电话，证明没有全局打通
        self.assertEqual(
            self.svc.view_contact("X3", "G-3", "B1", ["phone"], at=T_PHONE),
            {"phone": "+1-202-555-0103"})

    def test_proxy_only_region_gets_relay_reference_not_phone(self):
        scope = self.svc.allowed_fields("X5", "B3")
        self.assertEqual(scope, {"relay"})
        values = self.svc.view_contact("X5", None, "B3")
        self.assertEqual(values, {"relay": "relay-C8"})
        blob = json.dumps(values, ensure_ascii=False)
        self.assertNotIn("+1-202-555-0108", blob)
        # 即使显式要 phone，也拿不到原值，只会得到 relay 引用
        explicit = self.svc.view_contact("X5", None, "B3", ["relay"])
        self.assertEqual(explicit, {"relay": "relay-C8"})

    def test_old_grant_cannot_query_after_end_but_new_grant_can(self):
        with self.assertRaises(PermissionDenied):
            self.svc.view_contact("X3", "G-3", "B1", ["phone"], at=T_AFTER)
        with self.assertRaises(PermissionDenied):
            self.svc.view_contact("X3", "G-3A", "B1a", at=T_AFTER)
        # 新任务 G-4/B2 是当前委托，可以查其范围（drop_zone）
        self.assertEqual(
            self.svc.view_contact("X3", "G-4", "B2", at=T_AFTER),
            {"drop_zone": "E-row7"})

    def test_order_validity_period_enforced(self):
        with self.assertRaises(PermissionDenied):
            self.svc.view_contact("X3", "G-4", "B2", at=T_LATE)

    def test_current_delegation_follows_active_task(self):
        early = self.svc.current_delegation("X3", at=T_BEFORE)
        self.assertEqual(early["current"]["grant"], "G-3")
        middle = self.svc.current_delegation("X3", at=T_MID)
        self.assertEqual({g["grant"] for g in middle["active_grants"]}, {"G-3", "G-3A"})
        after = self.svc.current_delegation("X3", at=T_AFTER)
        self.assertEqual(after["current"]["grant"], "G-4")


class ProofsTest(unittest.TestCase):
    def setUp(self):
        self.svc = DeliveryService()

    def test_only_on_time_proof_is_valid(self):
        statuses = {p["receipt"]: p["validity"]["reason"] for p in self.svc.review_proofs()}
        self.assertTrue(self.svc.proof_validity("P3")["valid"])
        self.assertFalse(self.svc.proof_validity("P4")["valid"])
        self.assertEqual(statuses["P4"], "after_end")
        overview = self.svc.ops_overview(actor={"id": "ops-1", "roles": ["ops"]})
        self.assertEqual([p["receipt"] for p in overview["valid_proofs"]], ["P3"])

    def test_late_upload_of_early_signature_stays_valid(self):
        # 签收 01:50、上传 02:28：上传晚于改派不影响 P3 有效性
        proof = self.svc.proof_validity("P3")
        self.assertLess(proof["delivered_at"], proof["received_at"])
        self.assertTrue(proof["valid"])

    def test_old_carrier_can_submit_historical_proof_but_not_advance_task(self):
        result = self.svc.submit_proof(
            receipt="P9", order="X3", grant="G-3", partner="B1",
            delivered_at="2026-11-27T01:55:00Z", evidence="e-9",
            received_at="2026-11-27T03:10:00Z")
        self.assertTrue(result["validity"]["valid"])
        # 当前任务仍是 G-4，旧凭证提交没有改变分派
        self.assertEqual(self.svc.current_delegation("X3", at=T_AFTER)["current"]["grant"], "G-4")

    def test_invalid_proof_creates_manual_followup(self):
        self.svc.submit_proof(
            receipt="P10", order="X3", grant="G-3", partner="B1",
            delivered_at="2026-11-27T02:30:00Z", evidence="e-10")
        kinds = [(f["kind"], f["subject"]) for f in self.svc.followups()]
        self.assertIn(("proof_out_of_window", "P10"), kinds)

    def test_ops_authorization_enforced_for_named_non_ops_actor(self):
        with self.assertRaises(PermissionDenied):
            self.svc.ops_overview(actor={"id": "carrier-user", "roles": ["partner"]})
        # 本机受信上下文与 ops 角色可以
        self.svc.ops_overview()
        self.svc.ops_overview(actor={"id": "alice", "roles": ["ops"]})


class PeerConvergenceTest(unittest.TestCase):
    def setUp(self):
        self.server = serve_recording(RECORDING, 0)
        self.port = self.server.server_port
        self.thread = Thread(target=self.server.serve_forever,
                             kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()
        self.tmp = tempfile.TemporaryDirectory()
        self.state_path = os.path.join(self.tmp.name, "state.json")
        self.peer_url = f"http://127.0.0.1:{self.port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.tmp.cleanup()

    def make_service(self):
        return DeliveryService(self.state_path, peer_url=self.peer_url)

    def test_exact_d3_disclosure_stop_disconnect_and_two_stage_reconcile(self):
        svc = self.make_service()
        disc = svc.disclose("D-3", order="X3", grant="G-3", partner="B1",
                            fields=["phone"], at=T_PHONE)
        self.assertEqual(disc["status"], "active")
        self.assertEqual(disc["received_fields"], ["phone"])
        self.assertEqual(disc["peer_response"][0], 200)

        stopped = svc.request_stop("D-3", at=T_AFTER)
        # 对端 status 0：没收到应答，绝不能显示完成
        self.assertEqual(stopped["status"], "stop_pending")
        self.assertFalse(stopped["stop_acknowledged"])

        first = svc.reconcile()
        self.assertEqual(first["stopped"], [])
        self.assertIn("D-3", first["pending"])
        self.assertEqual(svc.get_disclosure("D-3")["peer_status"], "stop_pending")

        second = svc.reconcile()
        self.assertEqual(second["stopped"], ["D-3"])
        final = svc.get_disclosure("D-3")
        self.assertEqual(final["status"], "stopped")
        self.assertEqual(final["evidence"], "S4")
        self.assertTrue(final["converged_at"])

    def test_restart_resumes_convergence_and_state_has_no_values(self):
        svc = self.make_service()
        svc.disclose("D-3", order="X3", grant="G-3", partner="B1",
                     fields=["phone"], at=T_PHONE)
        svc.request_stop("D-3", at=T_AFTER)
        svc.close()

        # “重启”：同一路径新建服务实例
        svc2 = self.make_service()
        self.assertEqual(svc2.get_disclosure("D-3")["status"], "stop_pending")
        svc2.reconcile()  # 对端第一次 GET：stop_pending
        self.assertEqual(svc2.get_disclosure("D-3")["status"], "stop_pending")
        svc2.reconcile()  # 第二次 GET：stopped + S4
        self.assertEqual(svc2.get_disclosure("D-3")["status"], "stopped")
        self.assertEqual(svc2.get_disclosure("D-3")["evidence"], "S4")

        raw = Path(self.state_path).read_text(encoding="utf-8")
        self.assertNotIn("+1-202-555-0103", raw)
        audit = json.dumps(svc2.audit_entries(), ensure_ascii=False)
        self.assertNotIn("+1-202-555-0103", audit)
        self.assertIn('"phone"', audit)
        self.assertIn("S4", json.dumps(svc2.ops_overview(), ensure_ascii=False))

    def test_receiver_withdraw_makes_new_query_deny_and_stops_ref(self):
        svc = self.make_service()
        svc.disclose("D-3", order="X3", grant="G-3", partner="B1",
                     fields=["phone"], at=T_PHONE)
        updated = svc.update_choice("X3", ["drop_zone"], at=T_AFTER)
        self.assertIn("D-3", updated["stop_requested"])
        with self.assertRaises(PermissionDenied):
            svc.view_contact("X3", "G-3", "B1", ["phone"], at=T_AFTER)
        self.assertEqual(svc.get_disclosure("D-3")["status"], "stop_pending")

    def test_change_contact_uses_new_reference_immediately(self):
        svc = self.make_service()
        svc.upsert_contact({"contact_ref": "C-NEW", "phone": "+1-202-555-0199",
                            "drop_zone": "Z-9", "relay": "relay-NEW"})
        svc.update_choice("X4", ["phone", "drop_zone"], contact_ref="C-NEW")
        values = svc.view_contact("X4", None, "B1", ["phone"])
        self.assertEqual(values, {"phone": "+1-202-555-0199"})


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from app import create_server
        cls.tmp = tempfile.TemporaryDirectory()
        cls.state_path = os.path.join(cls.tmp.name, "http-state.json")
        cls.httpd = create_server("127.0.0.1", 0, state_path=cls.state_path,
                                  peer_url="http://127.0.0.1:9",
                                  reconcile_interval=0)
        cls.port = cls.httpd.server_port
        cls.thread = Thread(target=cls.httpd.serve_forever,
                            kwargs={"poll_interval": 0.02}, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.thread.join(timeout=2)
        cls.tmp.cleanup()

    def request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        payload = json.dumps(body).encode() if body is not None else None
        hdrs = headers or {}
        if payload is not None:
            hdrs.setdefault("Content-Type", "application/json")
        conn.request(method, path, body=payload, headers=hdrs)
        response = conn.getresponse()
        raw = response.read()
        conn.close()
        return response.status, json.loads(raw.decode()) if raw else None

    def test_health_and_dashboard(self):
        status, body = self.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertTrue(body["ok"])
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("GET", "/")
        resp = conn.getresponse()
        html = resp.read().decode()
        conn.close()
        self.assertEqual(resp.status, 200)
        self.assertIn("跨境交付", html)

    def test_contact_scope_over_http(self):
        status, body = self.request(
            "POST", "/v1/contact/view",
            {"order": "X4", "partner": "B1", "fields": ["phone"]})
        self.assertEqual(status, 403)
        self.assertEqual(body["error"], "permission_denied")
        status, body = self.request(
            "POST", "/v1/contact/view",
            {"order": "X3", "grant": "G-4", "partner": "B2", "at": T_AFTER})
        self.assertEqual(status, 200)
        self.assertEqual(body, {"drop_zone": "E-row7"})

    def test_dispatch_and_receiver_flow_over_http(self):
        status, body = self.request("GET", "/v1/orders/X3/delegation?at=" + T_AFTER, None)
        self.assertEqual(status, 200)
        self.assertEqual(body["current"]["grant"], "G-4")

        status, body = self.request(
            "POST", "/v1/dispatch",
            {"action": "assign", "order": "X5", "grant": "G-5",
             "partner": "B3", "at": T_BEFORE})
        self.assertEqual(status, 200)
        status, body = self.request(
            "POST", "/v1/number", {"order": "X5", "grant": "G-5", "partner": "B3",
                                   "at": T_BEFORE})
        self.assertEqual(status, 200)
        self.assertEqual(body, {"relay": "relay-C8"})

    def test_ops_overview_over_http(self):
        status, body = self.request("GET", "/v1/ops/overview", None,
                                    {"X-Actor-Id": "ops", "X-Actor-Role": "ops"})
        self.assertEqual(status, 200)
        self.assertIn("valid_proofs", body)
        receipts = [p["receipt"] for p in body["valid_proofs"]]
        self.assertEqual(receipts, ["P3"])

    def test_rpc_surface(self):
        status, body = self.request("POST", "/rpc",
                                   {"op": "allowed_fields",
                                    "args": {"order": "X5", "partner": "B3"}})
        self.assertEqual(status, 200)
        self.assertEqual(set(body), {"relay"})


class TaskAndPurposeTest(unittest.TestCase):
    def setUp(self):
        self.svc = DeliveryService()

    def test_old_grant_cannot_advance_task_but_new_grant_can(self):
        with self.assertRaises(PermissionDenied):
            self.svc.advance_task("X3", "G-3", "B1", "in_transit", at=T_AFTER)
        item = self.svc.advance_task("X3", "G-4", "B2", "accepted", at=T_AFTER)
        self.assertEqual(item["status"], "accepted")
        self.assertEqual(self.svc.task_status("X3")["partner"], "B2")
        # 被拒推进没有污染任务状态
        self.assertEqual(len([e for e in self.svc.task_events("X3")
                               if e["status"] == "in_transit"]), 0)

    def test_purpose_mismatch_denied(self):
        with self.assertRaises(PermissionDenied):
            self.svc.view_contact("X3", "G-3", "B1", ["phone"],
                                  at=T_PHONE, purpose="marketing")
        self.assertFalse(self.svc.check_access(
            "X3", "B1", ["phone"], "G-3", at=T_PHONE, purpose="marketing"))
        self.assertTrue(self.svc.check_access(
            "X3", "B1", ["phone"], "G-3", at=T_PHONE, purpose="handoff"))

    def test_new_assign_implicitly_supersedes_old_grant(self):
        self.svc.assign("X4", "G4A", "B1", at="2026-11-27T01:00:00Z")
        self.svc.assign("X4", "G4B", "B2", at="2026-11-27T03:00:00Z")
        self.assertTrue(self.svc.grant_active("X4", "G4A", "B1", at="2026-11-27T02:00:00Z"))
        self.assertFalse(self.svc.grant_active("X4", "G4A", "B1", at="2026-11-27T04:00:00Z"))
        self.assertTrue(self.svc.grant_active("X4", "G4B", "B2", at="2026-11-27T04:00:00Z"))
        with self.assertRaises(PermissionDenied):
            self.svc.advance_task("X4", "G4A", "B1", at="2026-11-27T04:00:00Z")

    def test_partner_must_be_registered_for_order_region(self):
        # B3 只登记在 R-Relay，不能用于 R-East 的 X3
        with self.assertRaises(PermissionDenied):
            self.svc.view_contact("X3", None, "B3")

    def test_delegate_command_validates_registered_chain(self):
        # 合规协力方 B1a(delegate_of=B1) 可挂在 G-3/B1 下
        row = self.svc.delegate("X3", "G-3X", "B1a", parent_grant="G-3", at=T_BEFORE)
        self.assertEqual(row["action"], "delegate")
        # 未登记方不能受委托
        with self.assertRaises(PermissionDenied):
            self.svc.delegate("X3", "G-GHOST", "GHOST", parent_grant="G-3", at=T_BEFORE)
        # 登记在别的委托方名下的协力方不能挂到 B1
        self.svc.register_partner(
            {"partner": "B9", "region": "R-East", "allowed": ["drop_zone"],
             "proxy_only": False, "delegate_of": "B2"})
        with self.assertRaises(PermissionDenied):
            self.svc.delegate("X3", "G-9", "B9", parent_grant="G-3", at=T_BEFORE)

    def test_dispatch_proof_retained_but_contact_still_filtered(self):
        allowed = self.svc.dispatch_proof("P3", grant="G-3", partner="B1", at=T_PHONE)
        self.assertTrue(allowed["valid"])
        self.assertEqual(allowed["current_contact"].get("phone"), "+1-202-555-0103")
        denied = self.svc.dispatch_proof("P3", grant="G-3", partner="B1", at=T_AFTER)
        self.assertTrue(denied["valid"])  # 凭证与有效性仍保留
        self.assertEqual(denied["current_contact"], {})
        self.assertEqual(denied["contact_denied_reason"], "grant_inactive")
        successor = self.svc.dispatch_proof("P3", grant="G-4", partner="B2", at=T_AFTER)
        self.assertEqual(successor["current_contact"], {"drop_zone": "E-row7"})

    def test_state_directory_and_url_first_arg(self):
        with tempfile.TemporaryDirectory() as directory:
            svc = DeliveryService(directory)
            svc.advance_task("X3", "G-4", "B2", "accepted", at=T_AFTER)
            self.assertTrue(Path(directory, "state.json").exists())
            svc2 = DeliveryService(directory)
            self.assertEqual(svc2.task_status("X3")["status"], "accepted")
        svc3 = DeliveryService("http://127.0.0.1:9")
        self.assertTrue(svc3.peer.base_url.endswith(":9"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
