import json, sys, tempfile, threading, unittest, urllib.request, urllib.error
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, SatelliteSchedulingService, create_server, iso, utcnow


def call(method, url, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


class ConcurrencyAndRbacTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(); cls.db = Path(cls.tmp.name) / "c.db"
        cls.server = create_server(cls.db, "127.0.0.1", 0)
        cls.port = cls.server.server_address[1]; cls.base = f"http://127.0.0.1:{cls.port}"
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True); cls.thread.start()
        t0 = utcnow().replace(microsecond=0) + timedelta(hours=1); cls.t0 = t0
        H = {"X-User-Id": "op", "X-Role": "operator"}
        call("POST", f"{cls.base}/api/satellites", {"id": "SAT1", "name": "s", "data_rate_mbps": 120, "priority": 5, "storage_capacity_mb": 1000000, "tenant": "T1"}, H)
        call("POST", f"{cls.base}/api/satellites", {"id": "SAT2", "name": "s2", "data_rate_mbps": 120, "priority": 5, "storage_capacity_mb": 1000000, "tenant": "T2"}, H)
        call("POST", f"{cls.base}/api/stations", {"id": "GS1", "name": "a"}, H)
        call("POST", f"{cls.base}/api/antennas", {"id": "ANT1", "station_id": "GS1", "max_rate_mbps": 80}, H)
        call("POST", f"{cls.base}/api/visibility-windows", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(t0), "ends_at": iso(t0 + timedelta(hours=2)), "max_rate_mbps": 80}, H)
        call("POST", f"{cls.base}/api/visibility-windows", {"satellite_id": "SAT2", "station_id": "GS1", "starts_at": iso(t0), "ends_at": iso(t0 + timedelta(hours=2)), "max_rate_mbps": 80}, H)
        call("POST", f"{cls.base}/api/quotas", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 100000}, H)
        call("POST", f"{cls.base}/api/quotas", {"tenant": "T2", "station_id": "GS1", "daily_seconds": 100000}, H)
        s, r1 = call("POST", f"{cls.base}/api/requests", {"satellite_id": "SAT1", "data_mb": 18000, "priority": 5, "deadline": iso(t0 + timedelta(days=1))}, {"X-User-Id": "u1", "X-Role": "requester", "X-Tenant": "T1"})
        s, r2 = call("POST", f"{cls.base}/api/requests", {"satellite_id": "SAT2", "data_mb": 10000, "priority": 5, "deadline": iso(t0 + timedelta(days=1))}, {"X-User-Id": "u2", "X-Role": "requester", "X-Tenant": "T2"})
        cls.rid1, cls.rid2 = r1["id"], r2["id"]

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown(); cls.server.server_close(); cls.tmp.cleanup()

    def test_concurrent_planners_first_writer_wins(self):
        barrier = threading.Barrier(2); results = {}
        H1 = {"X-User-Id": "planner-a", "X-Role": "operator"}
        H2 = {"X-User-Id": "planner-b", "X-Role": "operator"}
        # 排程员A：两段接力（0..30 与 50..70）覆盖整个请求；排程员B：与第一段重叠
        payload1 = {"segments": [
            {"window_id": 1, "antenna_id": "ANT1", "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(minutes=30)), "rate_mbps": 80},
            {"window_id": 1, "antenna_id": "ANT1", "starts_at": iso(self.t0 + timedelta(minutes=50)), "ends_at": iso(self.t0 + timedelta(minutes=70)), "rate_mbps": 80},
        ]}
        payload2 = {"window_id": 1, "antenna_id": "ANT1", "starts_at": iso(self.t0 + timedelta(minutes=10)), "ends_at": iso(self.t0 + timedelta(minutes=50)), "rate_mbps": 80}
        def submit(who, payload, hdr):
            barrier.wait()
            results[who] = call("POST", f"{self.base}/api/requests/{self.rid1}/schedule", payload, hdr)
        th = [threading.Thread(target=submit, args=("a", payload1, H1)), threading.Thread(target=submit, args=("b", payload2, H2))]
        for t in th: t.start()
        for t in th: t.join()
        statuses = sorted(s for s, _ in results.values())
        self.assertEqual(statuses, [201, 409])
        winner = "a" if results["a"][0] == 201 else "b"; loser = "b" if winner == "a" else "a"
        ok, conflict = results[winner][1], results[loser][1]
        winner_id = ok["segments"][0]["id"] if "segments" in ok else ok["id"]
        self.assertEqual(conflict["error"], "antenna_conflict")
        self.assertEqual(conflict["details"]["schedule_id"], winner_id)
        self.assertIn("revision", conflict["details"])
        self.assertGreaterEqual(conflict["details"]["revision"], 1)
        # 后到者看到冲突与最新版次后，改到不冲突的空闲时段为新请求排程，可以成立
        s, rid3 = call("POST", f"{self.base}/api/requests", {"satellite_id": "SAT1", "data_mb": 6000, "priority": 5, "deadline": iso(self.t0 + timedelta(days=1))}, {"X-User-Id": "u1", "X-Role": "requester", "X-Tenant": "T1"})
        self.assertEqual(s, 201)
        later = {"window_id": 1, "antenna_id": "ANT1", "starts_at": iso(self.t0 + timedelta(minutes=90)), "ends_at": iso(self.t0 + timedelta(minutes=100)), "rate_mbps": 80, "window_revision": 1}
        s, body = call("POST", f"{self.base}/api/requests/{rid3['id']}/schedule", later, {"X-User-Id": f"planner-{loser}", "X-Role": "operator"})
        self.assertEqual(s, 201, body)
        self.assertEqual(body["status"], "scheduled")

    def test_tenant_isolation_and_auditor_readonly(self):
        # T1 不能看 T2 的段
        s, state_t1 = call("GET", f"{self.base}/api/state", None, {"X-User-Id": "u1", "X-Role": "requester", "X-Tenant": "T1"})
        tenants = {r["tenant"] for r in state_t1["requests"]}
        self.assertEqual(tenants, {"T1"})
        for seg in state_t1["segments"]:
            owner = next(r for r in state_t1["requests"] if r["id"] == seg["request_id"])
            self.assertEqual(owner["tenant"], "T1")
        # auditor 只读：GET 放行，POST 全拒
        s, _ = call("GET", f"{self.base}/api/state", None, {"X-User-Id": "aud", "X-Role": "auditor"})
        self.assertEqual(s, 200)
        s, body = call("POST", f"{self.base}/api/stations", {"id": "GS9", "name": "x"}, {"X-User-Id": "aud", "X-Role": "auditor"})
        self.assertEqual(s, 403); self.assertEqual(body["error"], "read_only")
        s, body = call("POST", f"{self.base}/api/requests/1/schedule", {"window_id": 1, "antenna_id": "ANT1", "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(minutes=5)), "rate_mbps": 80}, {"X-User-Id": "aud", "X-Role": "auditor"})
        self.assertEqual(s, 403); self.assertEqual(body["error"], "read_only")
        # auditor 可看审计日志
        s, log = call("GET", f"{self.base}/api/audit-log", None, {"X-User-Id": "aud", "X-Role": "auditor"})
        self.assertEqual(s, 200); self.assertGreater(len(log["entries"]), 0)
        # 跨租户直接查段也被拒
        s, body = call("GET", f"{self.base}/api/schedules/1", None, {"X-User-Id": "u2", "X-Role": "requester", "X-Tenant": "T2"})
        self.assertIn(s, (403, 404))


if __name__ == "__main__": unittest.main()
