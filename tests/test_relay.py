import sys, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, SatelliteSchedulingService, iso, utcnow


class RelayTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = SatelliteSchedulingService(Path(self.tmp.name) / "test.db")
        self.t0 = utcnow().replace(microsecond=0) + timedelta(hours=1)
        self.svc.create_satellite("op", "operator", {"id": "SAT1", "name": "遥感一号", "data_rate_mbps": 120, "priority": 5, "storage_capacity_mb": 1000000, "tenant": "T1"})
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "clear"})
        self.svc.create_station("op", "operator", {"id": "GS2", "name": "喀什站", "weather": "clear"})
        self.svc.create_antenna("op", "operator", {"id": "ANT1", "station_id": "GS1", "max_rate_mbps": 80})
        self.svc.create_antenna("op", "operator", {"id": "ANT2", "station_id": "GS2", "max_rate_mbps": 80})
        # GS1 过站仅 30 分钟（提前），GS2 晚 40 分钟也可见
        self.w1 = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(minutes=30)), "max_rate_mbps": 80})
        self.w2 = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS2", "starts_at": iso(self.t0 + timedelta(minutes=40)), "ends_at": iso(self.t0 + timedelta(minutes=80)), "max_rate_mbps": 80})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 100000})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS2", "daily_seconds": 100000})

    def tearDown(self): self.tmp.cleanup()

    def req(self, mb=600, priority=5):
        return self.svc.create_request("u-t1", "requester", "T1", {"satellite_id": "SAT1", "data_mb": mb, "priority": priority, "deadline": iso(self.t0 + timedelta(days=1))})

    def test_relay_segments_split_across_stations(self):
        # 两段必须同时满足覆盖：GS1 30min@80 = 18000MB；请求 30000MB，单段不足
        req = self.req(30000)
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(req["id"], "op", "operator", {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(minutes=30)), "rate_mbps": 80})
        self.assertEqual(ctx.exception.code, "insufficient_capacity")
        out = self.svc.schedule_request(req["id"], "op", "operator", {"segments": [
            {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(minutes=30)), "rate_mbps": 80},
            {"window_id": self.w2["id"], "antenna_id": "ANT2", "starts_at": iso(self.t0 + timedelta(minutes=40)), "ends_at": iso(self.t0 + timedelta(minutes=60)), "rate_mbps": 80},
        ]})
        self.assertEqual(len(out["segments"]), 2)
        self.assertEqual([s["seq"] for s in out["segments"]], [1, 2])
        stations = {s["station_id"] for s in out["segments"]}
        self.assertEqual(stations, {"GS1", "GS2"})
        self.assertEqual(out["request"]["status"], "scheduled")
        with self.assertRaises(ApiError) as ctx:
            # 同一卫星不能同时段向两站下发：GS2 另有一个与 GS1 段重叠的窗口
            w3 = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS2", "starts_at": iso(self.t0 + timedelta(minutes=10)), "ends_at": iso(self.t0 + timedelta(minutes=25)), "max_rate_mbps": 80})
            self.svc.schedule_request(req["id"], "op", "operator", {"segments": [
                {"window_id": w3["id"], "antenna_id": "ANT2", "starts_at": iso(self.t0 + timedelta(minutes=10)), "ends_at": iso(self.t0 + timedelta(minutes=20)), "rate_mbps": 80},
            ]})
        self.assertEqual(ctx.exception.code, "satellite_conflict")

    def test_unfinished_data_hands_over_and_received_occupancy_kept(self):
        req = self.req(30000)
        out = self.svc.schedule_request(req["id"], "op", "operator", {"segments": [
            {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(minutes=30)), "rate_mbps": 80},
            {"window_id": self.w2["id"], "antenna_id": "ANT2", "starts_at": iso(self.t0 + timedelta(minutes=40)), "ends_at": iso(self.t0 + timedelta(minutes=60)), "rate_mbps": 80},
        ]})
        s1, s2 = out["segments"]
        # GS1 过站时间缩短：只完成 10 分钟收货 = 6000MB（计划 18000MB）
        self.svc.transition(s1["id"], "op", "operator", "", "receiving", {})
        self.svc.transition(s1["id"], "op", "operator", "", "received", {"received_mb": 6000})
        done = self.svc.get_schedule(s1["id"])
        self.assertEqual(done["status"], "received")
        self.assertEqual(done["received_mb"], 6000)
        # 已收段天线占用保留：同站同天线落在其窗口内的新段必须冲突
        blocker_req = self.req(5000)
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(blocker_req["id"], "op", "operator", {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.t0 + timedelta(minutes=5)), "ends_at": iso(self.t0 + timedelta(minutes=15)), "rate_mbps": 80})
        self.assertEqual(ctx.exception.code, "antenna_conflict")
        self.assertEqual(ctx.exception.details["schedule_id"], s1["id"])
        # 后续段（GS2）开始；请求仍是 receiving 而非 received（只再收 6000+8000=14000 < 30000）
        self.svc.transition(s2["id"], "op", "operator", "", "receiving", {})
        self.svc.transition(s2["id"], "op", "operator", "", "received", {"received_mb": 8000})
        state = {r["id"]: r for r in self.svc.state("operator", "")["requests"]}
        self.assertNotEqual(state[req["id"]]["status"], "received")
        self.assertEqual(state[req["id"]]["received_mb"], 14000)

    def test_window_change_recomputes_only_unstarted_segments(self):
        req = self.req(30000)
        out = self.svc.schedule_request(req["id"], "op", "operator", {"segments": [
            {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(minutes=30)), "rate_mbps": 80},
            {"window_id": self.w2["id"], "antenna_id": "ANT2", "starts_at": iso(self.t0 + timedelta(minutes=40)), "ends_at": iso(self.t0 + timedelta(minutes=60)), "rate_mbps": 80},
        ]})
        s1, s2 = out["segments"]
        # GS1 段先接收并完成 18000MB（占用保留）
        self.svc.transition(s1["id"], "op", "operator", "", "receiving", {})
        self.svc.transition(s1["id"], "op", "operator", "", "received", {"received_mb": 18000})
        # GS2 窗口恶化缩短：仅剩 40..50，且窗口版次将更新
        changed = self.svc.change_window(self.w2["id"], "op", "operator", {"starts_at": iso(self.t0 + timedelta(minutes=40)), "ends_at": iso(self.t0 + timedelta(minutes=50))})
        actions = {x["schedule_id"]: x for x in changed["impacts"]}
        self.assertEqual(actions[s1["id"]]["action"], "preserve_received_data")
        self.assertIn(actions[s2["id"]]["action"], {"replanned", "preempted"})
        self.assertEqual(changed["window"]["revision"], 2)
        # 过期版次提交必须被拒，并回传当前版次
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(req["id"], "op", "operator", {"window_id": self.w2["id"], "antenna_id": "ANT2", "starts_at": iso(self.t0 + timedelta(minutes=41)), "ends_at": iso(self.t0 + timedelta(minutes=49)), "rate_mbps": 80, "window_revision": 1})
        self.assertEqual(ctx.exception.code, "stale_window_revision")
        self.assertEqual(ctx.exception.details["current_revision"], 2)

    def test_bad_weather_invalidates_queued_segments_keeps_in_flight(self):
        req = self.req(24000)
        out = self.svc.schedule_request(req["id"], "op", "operator", {"segments": [
            {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(minutes=20)), "rate_mbps": 80},
            {"window_id": self.w2["id"], "antenna_id": "ANT2", "starts_at": iso(self.t0 + timedelta(minutes=40)), "ends_at": iso(self.t0 + timedelta(minutes=70)), "rate_mbps": 80},
        ]})
        s1, s2 = out["segments"]
        self.svc.transition(s1["id"], "op", "operator", "", "receiving", {})
        res = self.svc.set_station_weather("GS2", "op", "operator", {"weather": "storm"})
        kinds = {x["schedule_id"]: x["action"] for x in res["impacts"]}
        self.assertEqual(kinds[s1["id"]], "preserve_in_flight")
        self.assertEqual(kinds[s2["id"]], "preempted")  # 无更后窗口可接力
        # 天气未恢复前排程到 GS2 被拒绝
        req2 = self.req(4000)
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(req2["id"], "op", "operator", {"window_id": self.w2["id"], "antenna_id": "ANT2", "starts_at": iso(self.t0 + timedelta(minutes=42)), "ends_at": iso(self.t0 + timedelta(minutes=48)), "rate_mbps": 80})
        self.assertEqual(ctx.exception.code, "weather_blocked")

    def test_commander_priority_preempts_lower_queued_segment(self):
        low = self.req(12000, priority=3)
        low_out = self.svc.schedule_request(low["id"], "op", "operator", {"window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.t0), "ends_at": iso(self.t0 + timedelta(minutes=20)), "rate_mbps": 80})
        low_seg = low_out["id"] if "id" in low_out else low_out["segments"][0]
        urgent = self.req(6000, priority=9)
        res = self.svc.priority_preempt("cmd", "commander", {"request_id": urgent["id"], "window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.t0 + timedelta(minutes=5)), "ends_at": iso(self.t0 + timedelta(minutes=15)), "rate_mbps": 80, "order_id": "ORD-1", "reason": "应急侦察"})
        self.assertEqual(res["schedule"]["status"], "scheduled")
        self.assertEqual(res["displaced"][0]["schedule_id"], low_seg)
        # 被挤排队段自动在后续窗口（GS2）接力接收
        impact = {x["schedule_id"]: x for x in res["impacts"]}[low_seg]
        self.assertEqual(impact["action"], "replanned")
        self.assertEqual(self.svc.get_schedule(low_seg)["status"], "invalidated")
        relay = self.svc.get_schedule(impact["replacement_schedule_ids"][0])
        self.assertEqual(relay["station_id"], "GS2")
        self.assertEqual(relay["supersedes_id"], low_seg)
        # 同级请求不能抢占同级
        same = self.req(6000, priority=9)
        with self.assertRaises(ApiError) as ctx:
            self.svc.priority_preempt("cmd", "commander", {"request_id": same["id"], "window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.t0 + timedelta(minutes=6)), "ends_at": iso(self.t0 + timedelta(minutes=14)), "rate_mbps": 80})
        self.assertEqual(ctx.exception.code, "priority_conflict")
        self.assertEqual(ctx.exception.details["revision"], res["schedule"]["revision"])
        # 普通排程员不能调用抢占
        with self.assertRaises(ApiError) as ctx:
            self.svc.priority_preempt("op", "operator", {"request_id": same["id"], "window_id": self.w1["id"], "antenna_id": "ANT1", "starts_at": iso(self.t0 + timedelta(minutes=6)), "ends_at": iso(self.t0 + timedelta(minutes=14)), "rate_mbps": 80})
        self.assertEqual(ctx.exception.status, 403)


if __name__ == "__main__": unittest.main()
