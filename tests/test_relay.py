import sqlite3
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from app import ApiError, SatelliteSchedulingService, iso, utcnow


class RelayFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = SatelliteSchedulingService(Path(self.tmp.name) / "test.db")
        self.now = utcnow() + timedelta(hours=1)
        self.svc.create_satellite("op", "operator", {"id": "SAT1", "name": "遥感一号", "data_rate_mbps": 100, "priority": 8, "storage_capacity_mb": 100000, "tenant": "T1"})
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "clear"})
        self.svc.create_station("op", "operator", {"id": "GS2", "name": "上海站", "weather": "clear"})
        self.svc.create_antenna("op", "operator", {"id": "ANT1", "station_id": "GS1", "max_rate_mbps": 80})
        self.svc.create_antenna("op", "operator", {"id": "ANT2", "station_id": "GS2", "max_rate_mbps": 80})
        self.w1 = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=2)), "max_rate_mbps": 70})
        self.w2 = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS2", "starts_at": iso(self.now + timedelta(hours=3)), "ends_at": iso(self.now + timedelta(hours=5)), "max_rate_mbps": 70})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 86400})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS2", "daily_seconds": 86400})

    def tearDown(self):
        self.tmp.cleanup()

    def request(self, mb=80000, priority=7):
        return self.svc.create_request("requester-t1", "requester", "T1", {"satellite_id": "SAT1", "data_mb": mb, "priority": priority, "deadline": iso(self.now + timedelta(days=1))})

    def _start_receive(self, sid):
        self.svc.transition(sid, "op", "operator", "", "receiving", {})

    def _complete(self, sid):
        self.svc.transition(sid, "op", "operator", "", "receiving", {})
        self.svc.transition(sid, "op", "operator", "", "received", {})

    def test_relay_split_across_stations(self):
        req = self.request(80000)
        result = self.svc.relay_request(req["id"], "op", "operator", {"segments": [
            {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=2)), "rate_mbps": 70, "data_mb": 40000},
            {"window_id": self.w2["id"], "antenna_id": "ANT2", "starts_at": iso(self.now + timedelta(hours=3)), "ends_at": iso(self.now + timedelta(hours=5)), "rate_mbps": 70, "data_mb": 40000},
        ]})
        segs = result["segments"]
        self.assertEqual(len(segs), 2)
        self.assertEqual([s["segment_no"] for s in segs], [1, 2])
        self.assertEqual([s["station_id"] for s in segs], ["GS1", "GS2"])
        self.assertEqual([s["antenna_id"] for s in segs], ["ANT1", "ANT2"])
        self.assertEqual(result["request_status"], "scheduled")
        for s in segs:
            self.assertEqual(s["status"], "scheduled")
            self._complete(s["id"])
        self.assertEqual(self.svc.repo.conn.execute("SELECT status FROM requests WHERE id=?", (req["id"],)).fetchone()[0], "received")

    def test_relay_exceeding_remaining_rejected(self):
        req = self.request(80000)
        with self.assertRaises(ApiError) as ctx:
            self.svc.relay_request(req["id"], "op", "operator", {"segments": [
                {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=2)), "rate_mbps": 70, "data_mb": 90000},
            ]})
        self.assertEqual(ctx.exception.code, "relay_exceeds_remaining")

    def test_auto_allocate_to_later_windows(self):
        req = self.request(80000)
        result = self.svc.relay_request(req["id"], "op", "operator", {"auto": True})
        segs = result["segments"]
        self.assertEqual(len(segs), 2)
        self.assertEqual([s["station_id"] for s in segs], ["GS1", "GS2"])
        self.assertAlmostEqual(sum(s["data_mb"] for s in segs), 80000, delta=1)
        self.assertEqual(result["request_status"], "scheduled")

    def test_window_change_recomputes_unstarted_segment(self):
        req = self.request(20000)
        seg = self.svc.schedule_request(req["id"], "op", "operator", {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=1)), "rate_mbps": 70})
        changed = self.svc.change_window(self.w1["id"], "op", "operator", {"starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=30))})
        impact = next(i for i in changed["impacts"] if i["schedule_id"] == seg["id"])
        self.assertEqual(impact["action"], "preempted")
        self.assertEqual(self.svc.get_schedule(seg["id"])["status"], "preempted")
        self.assertEqual(len(impact["relay_segments"]), 1)
        new_seg = self.svc.get_schedule(impact["relay_segments"][0])
        self.assertEqual(new_seg["station_id"], "GS2")
        self.assertEqual(new_seg["status"], "scheduled")
        self.assertEqual(self.svc.repo.conn.execute("SELECT status FROM requests WHERE id=?", (req["id"],)).fetchone()[0], "scheduled")

    def test_window_change_preserves_received_and_relays_remainder(self):
        req = self.request(20000)
        seg = self.svc.schedule_request(req["id"], "op", "operator", {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=2)), "rate_mbps": 70})
        self._start_receive(seg["id"])
        changed = self.svc.change_window(self.w1["id"], "op", "operator", {"starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=30))})
        impact = next(i for i in changed["impacts"] if i["schedule_id"] == seg["id"])
        self.assertEqual(impact["action"], "relay_handover")
        self.assertAlmostEqual(impact["received_mb"], 15750, delta=1)
        kept = self.svc.get_schedule(seg["id"])
        self.assertEqual(kept["status"], "received")
        self.assertEqual(kept["antenna_id"], "ANT1")
        self.assertEqual(kept["station_id"], "GS1")
        self.assertAlmostEqual(kept["data_mb"], 15750, delta=1)
        self.assertEqual(len(impact["relay_segments"]), 1)
        new_seg = self.svc.get_schedule(impact["relay_segments"][0])
        self.assertEqual(new_seg["station_id"], "GS2")
        self.assertAlmostEqual(new_seg["data_mb"], 4250, delta=1)
        self.assertEqual(self.svc.repo.conn.execute("SELECT status FROM requests WHERE id=?", (req["id"],)).fetchone()[0], "relaying")

    def test_weather_change_recomputes_to_other_station(self):
        req = self.request(20000)
        seg = self.svc.schedule_request(req["id"], "op", "operator", {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=1)), "rate_mbps": 70})
        out = self.svc.set_weather("GS1", "op", "operator", {"weather": "storm"})
        impact = next(i for i in out["impacts"] if i["schedule_id"] == seg["id"])
        self.assertEqual(impact["action"], "preempted")
        self.assertEqual(self.svc.get_schedule(seg["id"])["status"], "preempted")
        self.assertEqual(len(impact["relay_segments"]), 1)
        self.assertEqual(self.svc.get_schedule(impact["relay_segments"][0])["station_id"], "GS2")
        self.assertEqual(self.svc.repo.conn.execute("SELECT status FROM requests WHERE id=?", (req["id"],)).fetchone()[0], "scheduled")

    def test_commander_priority_preemption(self):
        low = self.request(20000, priority=3)
        low_seg = self.svc.schedule_request(low["id"], "op", "operator", {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=1)), "rate_mbps": 70})
        out = self.svc.emergency_preempt(low_seg["id"], "commander", "commander", {"order_id": "O1", "reason": "紧急任务", "priority": 10})
        self.assertEqual(out["action"], "preempted")
        self.assertEqual(self.svc.get_schedule(low_seg["id"])["status"], "preempted")

        high = self.request(20000, priority=9)
        high_seg = self.svc.schedule_request(high["id"], "op", "operator", {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.now + timedelta(hours=1)), "ends_at": iso(self.now + timedelta(hours=2)), "rate_mbps": 70})
        with self.assertRaises(ApiError) as ctx:
            self.svc.emergency_preempt(high_seg["id"], "commander", "commander", {"order_id": "O2", "reason": "低优先级抢占", "priority": 5})
        self.assertEqual(ctx.exception.code, "priority_too_high")
        self.svc.emergency_preempt(high_seg["id"], "commander", "commander", {"order_id": "O3", "reason": "高优先级接管", "priority": 10})
        self.assertEqual(self.svc.get_schedule(high_seg["id"])["status"], "preempted")

    def test_occ_first_writer_wins_later_sees_latest_revision(self):
        version = self.svc._schedule_version(self.svc.repo.conn)
        self.svc.schedule_request(self.request(20000)["id"], "op", "operator", {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=1)), "rate_mbps": 70})
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(self.request(10000)["id"], "op2", "operator", {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.now + timedelta(minutes=10)), "ends_at": iso(self.now + timedelta(minutes=40)), "rate_mbps": 70, "if_match": version})
        self.assertEqual(ctx.exception.code, "revision_conflict")
        self.assertEqual(ctx.exception.details["latest_revision"], version + 1)
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(self.request(10000)["id"], "op2", "operator", {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.now + timedelta(minutes=10)), "ends_at": iso(self.now + timedelta(minutes=40)), "rate_mbps": 70})
        self.assertEqual(ctx.exception.code, "antenna_conflict")
        self.assertEqual(ctx.exception.details["latest_revision"], version + 1)

    def test_tenant_only_sees_own_relay_segments(self):
        req = self.request(20000)
        seg = self.svc.schedule_request(req["id"], "op", "operator", {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=1)), "rate_mbps": 70})
        self.assertEqual(self.svc.get_schedule(seg["id"], "requester", "T1")["id"], seg["id"])
        with self.assertRaises(ApiError) as ctx:
            self.svc.get_schedule(seg["id"], "requester", "T2")
        self.assertEqual(ctx.exception.status, 404)
        state_t1 = self.svc.state("requester", "T1")
        self.assertTrue(all(s["request_id"] == req["id"] for s in state_t1["schedules"]))
        state_t2 = self.svc.state("requester", "T2")
        self.assertEqual(state_t2["schedules"], [])

    def test_auditor_read_only(self):
        self.assertEqual(self.svc.state("auditor", "")["schedules"] is not None, True)
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_satellite("aud", "auditor", {"id": "SATX", "name": "X", "data_rate_mbps": 10, "priority": 5, "storage_capacity_mb": 100, "tenant": "T1"})
        self.assertEqual(ctx.exception.code, "resource_forbidden")
        with self.assertRaises(ApiError) as ctx:
            self.svc.reschedule(1, "aud", "auditor", {})
        self.assertEqual(ctx.exception.code, "auditor_readonly")

    def test_legacy_schema_migrates_to_segments(self):
        legacy = Path(self.tmp.name) / "legacy.db"
        conn = sqlite3.connect(str(legacy))
        conn.executescript("""
        CREATE TABLE satellites(id TEXT PRIMARY KEY, name TEXT, data_rate_mbps REAL, priority INTEGER, storage_capacity_mb REAL, tenant TEXT, status TEXT);
        CREATE TABLE stations(id TEXT PRIMARY KEY, name TEXT, status TEXT, weather TEXT);
        CREATE TABLE antennas(id TEXT PRIMARY KEY, station_id TEXT, max_rate_mbps REAL, status TEXT);
        CREATE TABLE visibility_windows(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT, station_id TEXT, starts_at TEXT, ends_at TEXT, max_rate_mbps REAL, revision INTEGER DEFAULT 1);
        CREATE TABLE requests(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT, tenant TEXT, priority INTEGER, data_mb REAL, deadline TEXT, status TEXT, created_by TEXT, created_at TEXT);
        CREATE TABLE schedules(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER UNIQUE, window_id INTEGER, station_id TEXT, antenna_id TEXT, satellite_id TEXT, starts_at TEXT, ends_at TEXT, rate_mbps REAL, status TEXT DEFAULT 'scheduled', revision INTEGER DEFAULT 1, disposition_reason TEXT, created_by TEXT, created_at TEXT, updated_at TEXT);
        INSERT INTO satellites VALUES('SAT1','遥感一号',100,8,100000,'T1','active');
        INSERT INTO stations VALUES('GS1','北京站','active','clear');
        INSERT INTO antennas VALUES('ANT1','GS1',80,'active');
        INSERT INTO visibility_windows(satellite_id,station_id,starts_at,ends_at,max_rate_mbps) VALUES('SAT1','GS1','2026-10-03T10:00:00Z','2026-10-03T12:00:00Z',70);
        INSERT INTO requests(satellite_id,tenant,priority,data_mb,deadline,status,created_by,created_at) VALUES('SAT1','T1',7,20000,'2026-10-04T10:00:00Z','pending','u','2026-10-03T09:00:00Z');
        INSERT INTO schedules(request_id,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,status,created_by,created_at,updated_at) VALUES(1,1,'GS1','ANT1','SAT1','2026-10-03T10:00:00Z','2026-10-03T11:00:00Z',70,'scheduled','u','2026-10-03T09:00:00Z','2026-10-03T09:00:00Z');
        """)
        conn.commit(); conn.close()
        svc = SatelliteSchedulingService(legacy)
        seg = svc.get_schedule(1)
        self.assertEqual(seg["segment_no"], 1)
        self.assertAlmostEqual(seg["data_mb"], 31500, delta=1)
        # legacy UNIQUE(request_id) must be gone so a request can hold multiple relay segments
        svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": "2026-10-03T13:00:00Z", "ends_at": "2026-10-03T15:00:00Z", "max_rate_mbps": 70})
        svc.schedule_request(1, "op", "operator", {"window_id": 2, "antenna_id": "ANT1", "starts_at": "2026-10-03T13:00:00Z", "ends_at": "2026-10-03T14:00:00Z", "rate_mbps": 70})
        self.assertEqual(svc.repo.conn.execute("SELECT COUNT(*) FROM schedules WHERE request_id=1").fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()
