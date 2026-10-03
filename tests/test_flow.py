import sys, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, SatelliteSchedulingService, iso, utcnow


class SatelliteFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = SatelliteSchedulingService(Path(self.tmp.name) / "test.db"); self.now = utcnow() + timedelta(hours=1)
        self.svc.create_satellite("op", "operator", {"id": "SAT1", "name": "遥感一号", "data_rate_mbps": 100, "priority": 8, "storage_capacity_mb": 100000, "tenant": "T1"})
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "clear"})
        self.svc.create_antenna("op", "operator", {"id": "ANT1", "station_id": "GS1", "max_rate_mbps": 80})
        self.window = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=2)), "max_rate_mbps": 70})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 7200})

    def tearDown(self): self.tmp.cleanup()

    def request(self, mb=35000):
        return self.svc.create_request("requester-t1", "requester", "T1", {"satellite_id": "SAT1", "data_mb": mb, "priority": 7, "deadline": iso(self.now + timedelta(days=1))})

    def test_complete_receive_and_window_change_impact(self):
        req = self.request(); schedule = self.svc.schedule_request(req["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=1, minutes=30)), "rate_mbps": 60})
        self.assertEqual(schedule["status"], "scheduled")
        self.svc.transition(schedule["id"], "op", "operator", "", "receiving", {})
        self.svc.transition(schedule["id"], "op", "operator", "", "received", {})
        req2 = self.request(10000)
        schedule2 = self.svc.schedule_request(req2["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now + timedelta(hours=1)), "ends_at": iso(self.now + timedelta(hours=1, minutes=30)), "rate_mbps": 50})
        changed = self.svc.change_window(self.window["id"], "op", "operator", {"starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=1, minutes=10))})
        impacts = {x["schedule_id"]: x for x in changed["impacts"]}
        self.assertEqual(impacts[schedule["id"]]["action"], "preserve_received_data")
        self.assertEqual(impacts[schedule2["id"]]["action"], "preempted")
        self.assertEqual(self.svc.get_schedule(schedule2["id"])["status"], "preempted")

    def test_conflicts_permissions_and_data_protection(self):
        req = self.request(10000); schedule = self.svc.schedule_request(req["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=30)), "rate_mbps": 50})
        req2 = self.request(10000)
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(req2["id"], "requester-t1", "requester", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=20)), "rate_mbps": 50})
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(req2["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now + timedelta(minutes=10)), "ends_at": iso(self.now + timedelta(minutes=40)), "rate_mbps": 50})
        self.assertEqual(ctx.exception.code, "antenna_conflict")
        self.svc.transition(schedule["id"], "op", "operator", "", "receiving", {})
        self.svc.transition(schedule["id"], "op", "operator", "", "received", {})
        with self.assertRaises(ApiError) as ctx:
            self.svc.cancel_schedule(schedule["id"], "op", "operator", "", {"reason": "测试"})
        self.assertEqual(ctx.exception.code, "received_data_protected")


if __name__ == "__main__": unittest.main()
