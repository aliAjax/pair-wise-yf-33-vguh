#!/usr/bin/env python3
"""Multi-station satellite scheduling service using only Python's standard library.

Supports splitting one data request into relay segments (接力段) across multiple
ground stations: each station owns one segment, unfinished data rolls over to
later visibility windows, already received data and antenna occupancy are kept,
window changes invalidate and recompute only unstarted segments, commanders
preempt queued segments by priority, concurrent planners use first-writer-wins
with revision feedback, tenants see only their own segments and auditors are
read-only.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

PORT = 8204
ROLES = {"viewer", "requester", "operator", "commander", "auditor"}
WRITE_ROLES = {"operator", "commander"}
# Segment statuses: scheduled(排队/已排程未开始) receiving received(已收,占用保留)
# preempted(被抢占,等待接力) invalidated(窗口失效,已由新段替代) canceled
ACTIVE_STATUSES = ("scheduled", "receiving")
OCCUPYING_STATUSES = ("scheduled", "receiving", "received")
EPS = 1e-6


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(message); self.status, self.code, self.message, self.details = status, code, message, details


def utcnow() -> datetime: return datetime.now(timezone.utc)
def iso(value: datetime | None = None) -> str: return (value or utcnow()).replace(microsecond=0).isoformat().replace("+00:00", "Z")
def parse_time(value: str | None) -> datetime:
    if not value: raise ApiError(400, "time_required", "必须提供 ISO 8601 时间")
    try: parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc: raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
def overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool: return a_start < b_end and b_start < a_end
def readonly(role: str) -> None:
    if role == "auditor": raise ApiError(403, "read_only", "审计角色只能查看，不能修改")


class Repository:
    def __init__(self, path: str | Path):
        self.path = str(path)
        self._local = threading.local()
        # 主线程连接负责一次性建表/迁移；其他线程按需创建
        self._schema(self.connect())

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self.connect(); self._local.conn = conn
        return conn

    def _schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS satellites(id TEXT PRIMARY KEY, name TEXT NOT NULL, data_rate_mbps REAL NOT NULL, priority INTEGER NOT NULL, storage_capacity_mb REAL NOT NULL, tenant TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE IF NOT EXISTS stations(id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', weather TEXT NOT NULL DEFAULT 'clear');
        CREATE TABLE IF NOT EXISTS antennas(id TEXT PRIMARY KEY, station_id TEXT NOT NULL REFERENCES stations(id), max_rate_mbps REAL NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE IF NOT EXISTS maintenance(id INTEGER PRIMARY KEY AUTOINCREMENT, station_id TEXT NOT NULL REFERENCES stations(id), antenna_id TEXT REFERENCES antennas(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, reason TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS visibility_windows(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT NOT NULL REFERENCES satellites(id), station_id TEXT NOT NULL REFERENCES stations(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, max_rate_mbps REAL NOT NULL, revision INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT NOT NULL REFERENCES satellites(id), tenant TEXT NOT NULL, priority INTEGER NOT NULL, data_mb REAL NOT NULL, received_mb REAL NOT NULL DEFAULT 0, deadline TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_by TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS quotas(id INTEGER PRIMARY KEY AUTOINCREMENT, tenant TEXT NOT NULL, station_id TEXT NOT NULL REFERENCES stations(id), daily_seconds INTEGER NOT NULL, UNIQUE(tenant,station_id));
        CREATE TABLE IF NOT EXISTS schedules(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER NOT NULL REFERENCES requests(id), seq INTEGER NOT NULL DEFAULT 1, window_id INTEGER NOT NULL REFERENCES visibility_windows(id), station_id TEXT NOT NULL REFERENCES stations(id), antenna_id TEXT NOT NULL REFERENCES antennas(id), satellite_id TEXT NOT NULL REFERENCES satellites(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, rate_mbps REAL NOT NULL, planned_mb REAL NOT NULL DEFAULT 0, received_mb REAL NOT NULL DEFAULT 0, supersedes_id INTEGER REFERENCES schedules(id), status TEXT NOT NULL DEFAULT 'scheduled', revision INTEGER NOT NULL DEFAULT 1, disposition_reason TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER, schedule_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
        """)
        self._migrate(conn)

    def _migrate(self, conn: sqlite3.Connection) -> None:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(requests)")}
        if "received_mb" not in cols:
            conn.execute("ALTER TABLE requests ADD COLUMN received_mb REAL NOT NULL DEFAULT 0")
        cols = {r[1] for r in conn.execute("PRAGMA table_info(schedules)")}
        for name, ddl in (("seq", "ALTER TABLE schedules ADD COLUMN seq INTEGER NOT NULL DEFAULT 1"),
                          ("planned_mb", "ALTER TABLE schedules ADD COLUMN planned_mb REAL NOT NULL DEFAULT 0"),
                          ("received_mb", "ALTER TABLE schedules ADD COLUMN received_mb REAL NOT NULL DEFAULT 0"),
                          ("supersedes_id", "ALTER TABLE schedules ADD COLUMN supersedes_id INTEGER")):
            if name not in cols: conn.execute(ddl)

    @contextmanager
    def tx(self):
        # IMMEDIATE 取写锁：两个排程员并发提交时严格串行，先到者先写
        self.conn.execute("BEGIN IMMEDIATE")
        try: yield self.conn; self.conn.execute("COMMIT")
        except Exception: self.conn.execute("ROLLBACK"); raise

    @staticmethod
    def audit(conn: sqlite3.Connection, request_id: int | None, schedule_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(request_id,schedule_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?,?)",
                     (request_id, schedule_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


class SatelliteSchedulingService:
    def __init__(self, path: str | Path): self.repo = Repository(path)

    @staticmethod
    def identity(headers: Any) -> tuple[str, str, str]:
        actor, role, tenant = headers.get("X-User-Id", "").strip(), headers.get("X-Role", "").strip(), headers.get("X-Tenant", "").strip()
        if not actor or role not in ROLES: raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效 X-Role")
        if role == "requester" and not tenant: raise ApiError(401, "tenant_required", "requester 必须提供 X-Tenant")
        return actor, role, tenant

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None: return dict(row) if row else None

    # ------------------------------------------------------------------ resources
    def create_satellite(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        readonly(role)
        if role not in WRITE_ROLES: raise ApiError(403, "resource_forbidden", "当前角色不能维护卫星")
        sid, name, tenant = str(body.get("id", "")).strip(), str(body.get("name", "")).strip(), str(body.get("tenant", "")).strip()
        rate, priority, capacity = body.get("data_rate_mbps"), body.get("priority"), body.get("storage_capacity_mb")
        if not sid or not name or not tenant or not isinstance(rate, (int, float)) or float(rate) <= 0 or not isinstance(priority, int) or not 1 <= priority <= 10 or not isinstance(capacity, (int, float)) or float(capacity) <= 0:
            raise ApiError(400, "invalid_satellite", "卫星参数不完整")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO satellites(id,name,data_rate_mbps,priority,storage_capacity_mb,tenant,status) VALUES(?,?,?,?,?,?,?)", (sid, name, float(rate), priority, float(capacity), tenant, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM satellites WHERE id=?", (sid,)).fetchone())

    def create_station(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        readonly(role)
        if role not in WRITE_ROLES: raise ApiError(403, "resource_forbidden", "当前角色不能维护地面站")
        sid, name = str(body.get("id", "")).strip(), str(body.get("name", "")).strip()
        weather = str(body.get("weather", "clear")).lower()
        if not sid or not name or weather not in {"clear", "rain", "storm", "closed"}: raise ApiError(400, "invalid_station", "地面站名称或天气状态无效")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO stations(id,name,status,weather) VALUES(?,?,?,?)", (sid, name, body.get("status", "active"), weather))
            return dict(conn.execute("SELECT * FROM stations WHERE id=?", (sid,)).fetchone())

    def set_station_weather(self, station_id: str, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        """天气转坏：未开始段立即失效并在后续可用窗口重算；接收中/已收保留。"""
        readonly(role)
        if role not in WRITE_ROLES: raise ApiError(403, "weather_forbidden", "当前角色不能更新天气")
        weather = str(body.get("weather", "")).strip().lower()
        if weather not in {"clear", "rain", "storm", "closed"}: raise ApiError(400, "invalid_weather", "天气状态无效")
        with self.repo.tx() as conn:
            station = conn.execute("SELECT * FROM stations WHERE id=?", (station_id,)).fetchone()
            if not station: raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("UPDATE stations SET weather=? WHERE id=?", (weather, station_id))
            impacts: list[dict[str, Any]] = []
            if weather != "clear":
                rows = conn.execute("SELECT * FROM schedules WHERE station_id=? AND status IN ('scheduled','receiving','received') ORDER BY starts_at", (station_id,)).fetchall()
                victims = []
                for row in rows:
                    if row["status"] == "scheduled":
                        victims.append(row)
                    else:
                        impacts.append({"schedule_id": row["id"], "request_id": row["request_id"], "action": "preserve_received_data" if row["status"] == "received" else "preserve_in_flight", "reason": "天气转坏时已收数据和接收中任务保留"})
                replanned = self._invalidate_and_replan(conn, victims, actor, role, f"weather_{weather}", skip_station=station_id)
                # 受影响请求在其他站上的接收中/已收段同样只保留、不重算
                touched = {v["request_id"] for v in victims}
                for rid in touched:
                    for row in conn.execute("SELECT * FROM schedules WHERE request_id=? AND status IN ('received','receiving') AND station_id!=? ORDER BY id", (rid, station_id)).fetchall():
                        impacts.append({"schedule_id": row["id"], "request_id": rid, "action": "preserve_received_data" if row["status"] == "received" else "preserve_in_flight", "reason": "已收数据/接收中段不受天气变化影响"})
                impacts[0:0] = replanned
            Repository.audit(conn, None, None, actor, role, "station_weather_changed", {"station_id": station_id, "weather": weather, "impacts": impacts})
            return {"station": dict(conn.execute("SELECT * FROM stations WHERE id=?", (station_id,)).fetchone()), "impacts": impacts}

    def create_antenna(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        readonly(role)
        if role not in WRITE_ROLES: raise ApiError(403, "resource_forbidden", "当前角色不能维护天线")
        aid, station, rate = str(body.get("id", "")).strip(), str(body.get("station_id", "")).strip(), body.get("max_rate_mbps")
        if not aid or not station or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_antenna", "天线参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("INSERT OR REPLACE INTO antennas(id,station_id,max_rate_mbps,status) VALUES(?,?,?,?)", (aid, station, float(rate), body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM antennas WHERE id=?", (aid,)).fetchone())

    def create_maintenance(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        readonly(role)
        if role not in WRITE_ROLES: raise ApiError(403, "maintenance_forbidden", "当前角色不能登记维护")
        station, start, end, reason = str(body.get("station_id", "")).strip(), parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), str(body.get("reason", "")).strip()
        antenna = body.get("antenna_id")
        if not station or end <= start or not reason: raise ApiError(400, "invalid_maintenance", "维护参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            if antenna and not conn.execute("SELECT 1 FROM antennas WHERE id=? AND station_id=?", (antenna, station)).fetchone(): raise ApiError(400, "antenna_station_mismatch", "天线不属于该站")
            cur = conn.execute("INSERT INTO maintenance(station_id,antenna_id,starts_at,ends_at,reason) VALUES(?,?,?,?,?)", (station, antenna, iso(start), iso(end), reason))
            return dict(conn.execute("SELECT * FROM maintenance WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_window(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        readonly(role)
        if role not in WRITE_ROLES: raise ApiError(403, "window_forbidden", "当前角色不能维护可见窗口")
        satellite, station, start, end, rate = str(body.get("satellite_id", "")).strip(), str(body.get("station_id", "")).strip(), parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), body.get("max_rate_mbps")
        if not satellite or not station or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_window", "可见窗口参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM satellites WHERE id=?", (satellite,)).fetchone(): raise ApiError(404, "satellite_not_found", "卫星不存在")
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            cur = conn.execute("INSERT INTO visibility_windows(satellite_id,station_id,starts_at,ends_at,max_rate_mbps) VALUES(?,?,?,?,?)", (satellite, station, iso(start), iso(end), float(rate)))
            return dict(conn.execute("SELECT * FROM visibility_windows WHERE id=?", (cur.lastrowid,)).fetchone())

    def set_quota(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        readonly(role)
        if role not in WRITE_ROLES: raise ApiError(403, "quota_forbidden", "当前角色不能设置租户配额")
        tenant, station, seconds = str(body.get("tenant", "")).strip(), str(body.get("station_id", "")).strip(), body.get("daily_seconds")
        if not tenant or not station or not isinstance(seconds, int) or seconds <= 0: raise ApiError(400, "invalid_quota", "配额参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("INSERT INTO quotas(tenant,station_id,daily_seconds) VALUES(?,?,?) ON CONFLICT(tenant,station_id) DO UPDATE SET daily_seconds=excluded.daily_seconds", (tenant, station, seconds))
            return dict(conn.execute("SELECT * FROM quotas WHERE tenant=? AND station_id=?", (tenant, station)).fetchone())

    def create_request(self, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
        readonly(role)
        if role not in {"requester", "operator", "commander"}: raise ApiError(403, "request_forbidden", "当前角色不能创建数据请求")
        satellite = str(body.get("satellite_id", "")).strip(); data_mb = body.get("data_mb"); priority = body.get("priority"); deadline = parse_time(body.get("deadline"))
        owner = tenant if role == "requester" else str(body.get("tenant", "")).strip()
        if not satellite or not owner or not isinstance(data_mb, (int, float)) or float(data_mb) <= 0 or not isinstance(priority, int) or not 1 <= priority <= 10:
            raise ApiError(400, "invalid_request", "请求参数无效")
        with self.repo.tx() as conn:
            satellite_row = conn.execute("SELECT * FROM satellites WHERE id=?", (satellite,)).fetchone()
            if not satellite_row: raise ApiError(404, "satellite_not_found", "卫星不存在")
            if owner != satellite_row["tenant"]: raise ApiError(403, "tenant_satellite_forbidden", "租户不能申请不属于自己的卫星")
            if deadline <= utcnow(): raise ApiError(409, "deadline_expired", "请求截止时间已经过去")
            cur = conn.execute("INSERT INTO requests(satellite_id,tenant,priority,data_mb,deadline,created_by,created_at) VALUES(?,?,?,?,?,?,?)", (satellite, owner, priority, float(data_mb), iso(deadline), actor, iso()))
            request_id = cur.lastrowid; Repository.audit(conn, request_id, None, actor, role, "request_created", {"data_mb": float(data_mb), "deadline": iso(deadline)})
            return dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone())

    # ------------------------------------------------------------------ planning
    def _used_quota(self, conn: sqlite3.Connection, tenant: str, station: str, day: str, exclude_ids: set[int] | None = None) -> int:
        sql = """SELECT COALESCE(SUM((julianday(s.ends_at)-julianday(s.starts_at))*86400),0) FROM schedules s JOIN requests r ON r.id=s.request_id
                 WHERE r.tenant=? AND s.station_id=? AND substr(s.starts_at,1,10)=? AND s.status IN ('scheduled','receiving','received')"""
        args: list[Any] = [tenant, station, day]
        if exclude_ids:
            sql += " AND s.id NOT IN (%s)" % ",".join("?" * len(exclude_ids)); args += list(exclude_ids)
        return int(conn.execute(sql, args).fetchone()[0])

    @staticmethod
    def _maintenance(conn: sqlite3.Connection, station: str, antenna: str, start_s: str, end_s: str) -> sqlite3.Row | None:
        return conn.execute("""SELECT * FROM maintenance WHERE station_id=? AND (antenna_id IS NULL OR antenna_id=?) AND starts_at<? AND ends_at>?""", (station, antenna, end_s, start_s)).fetchone()

    def _occupancy(self, conn: sqlite3.Connection, station: str, antenna: str, satellite: str, start_s: str, end_s: str, exclude: set[int]) -> sqlite3.Row | None:
        """返回占用冲突段（天线占用或同星他站接收），含请求优先级和最新版次。"""
        marks = ",".join("?" * len(exclude)) if exclude else "-1"
        sql = f"""SELECT s.*, r.priority AS request_priority, r.tenant AS tenant FROM schedules s JOIN requests r ON r.id=s.request_id
                  WHERE s.status IN ('scheduled','receiving','received') AND s.starts_at<? AND s.ends_at>? AND s.id NOT IN ({marks})
                  AND ((s.station_id=? AND s.antenna_id=?) OR s.satellite_id=?) LIMIT 1"""
        args: list[Any] = [end_s, start_s] + list(exclude) + [station, antenna, satellite]
        return conn.execute(sql, args).fetchone()

    @staticmethod
    def _conflict_detail(row: sqlite3.Row) -> dict[str, Any]:
        return {"schedule_id": row["id"], "revision": row["revision"], "status": row["status"], "request_priority": row["request_priority"], "updated_at": row["updated_at"]}

    def _parse_legs(self, conn: sqlite3.Connection, body: dict[str, Any]) -> list[dict[str, Any]]:
        raw = body["segments"] if isinstance(body.get("segments"), list) else [body]
        if not raw: raise ApiError(400, "segments_required", "至少提供一个接力段")
        legs: list[dict[str, Any]] = []
        for i, item in enumerate(raw):
            window_id, antenna_id = item.get("window_id"), str(item.get("antenna_id", "")).strip()
            start, end, rate = parse_time(item.get("starts_at")), parse_time(item.get("ends_at")), item.get("rate_mbps")
            if not isinstance(window_id, int) or not antenna_id or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0:
                raise ApiError(400, "invalid_schedule", f"第 {i + 1} 个接力段参数无效")
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
            antenna = conn.execute("SELECT * FROM antennas WHERE id=?", (antenna_id,)).fetchone()
            if not window or not antenna: raise ApiError(404, "schedule_ref_not_found", f"第 {i + 1} 段的窗口或天线不存在")
            expected_rev = item.get("window_revision")
            if expected_rev is not None and int(expected_rev) != int(window["revision"]):
                raise ApiError(409, "stale_window_revision", f"第 {i + 1} 段依据的窗口版次已过期", {"window_id": window_id, "current_revision": window["revision"]})
            legs.append({"window": window, "antenna": antenna, "start": start, "end": end, "rate": float(rate)})
        return legs

    def _validate_leg(self, conn: sqlite3.Connection, request: sqlite3.Row, satellite: sqlite3.Row, leg: dict[str, Any], exclude: set[int]) -> None:
        window, antenna, start, end, rate = leg["window"], leg["antenna"], leg["start"], leg["end"], leg["rate"]
        station = conn.execute("SELECT * FROM stations WHERE id=?", (window["station_id"],)).fetchone()
        if request["satellite_id"] != window["satellite_id"] or window["station_id"] != antenna["station_id"]: raise ApiError(409, "window_mismatch", "卫星、窗口和天线不匹配")
        if satellite["status"] != "active" or station["status"] != "active" or antenna["status"] != "active": raise ApiError(409, "resource_inactive", "卫星、地面站或天线不可用")
        if station["weather"] != "clear": raise ApiError(409, "weather_blocked", f"地面站 {station['id']} 天气条件不允许接收")
        w_start, w_end = parse_time(window["starts_at"]), parse_time(window["ends_at"])
        if start < w_start or end > w_end: raise ApiError(409, "outside_visibility", "排程超出可见窗口")
        if end > parse_time(request["deadline"]): raise ApiError(409, "deadline_missed", "预计结束时间超过请求截止时间")
        max_rate = min(float(satellite["data_rate_mbps"]), float(window["max_rate_mbps"]), float(antenna["max_rate_mbps"]))
        if rate > max_rate: raise ApiError(409, "rate_exceeded", "请求速率超过可用上限", {"max_rate_mbps": max_rate})
        if self._maintenance(conn, station["id"], antenna["id"], iso(start), iso(end)): raise ApiError(409, "maintenance_conflict", "天线或地面站处于维护期")
        # 与本批之外已有占用（含本请求已有接力段）的重叠：天线占用或同星他站/同时段
        blocker = self._occupancy(conn, station["id"], antenna["id"], request["satellite_id"], iso(start), iso(end), exclude)
        if blocker:
            code = "antenna_conflict" if blocker["station_id"] == station["id"] and blocker["antenna_id"] == antenna["id"] else "satellite_conflict"
            raise ApiError(409, code, "时段已被占用，先写入的排程成立", self._conflict_detail(blocker))

    def _insert_segment(self, conn: sqlite3.Connection, request: sqlite3.Row, leg: dict[str, Any], seq: int, planned_mb: float, actor: str, action: str, supersedes_id: int | None = None) -> int:
        window, antenna, start, end, rate = leg["window"], leg["antenna"], leg["start"], leg["end"], leg["rate"]
        cur = conn.execute("""INSERT INTO schedules(request_id,seq,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,planned_mb,supersedes_id,created_by,created_at,updated_at)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                           (request["id"], seq, window["id"], window["station_id"], antenna["id"], request["satellite_id"], iso(start), iso(end), rate, planned_mb, supersedes_id, actor, iso(), iso()))
        Repository.audit(conn, request["id"], cur.lastrowid, actor, "commander" if action.startswith("priority") else "operator", action,
                         {"window_id": window["id"], "station_id": window["station_id"], "antenna_id": antenna["id"], "seq": seq, "planned_mb": planned_mb, "supersedes_id": supersedes_id})
        return cur.lastrowid

    def _active_planned(self, conn: sqlite3.Connection, request_id: int) -> float:
        return float(conn.execute("SELECT COALESCE(SUM(planned_mb),0) FROM schedules WHERE request_id=? AND status IN ('scheduled','receiving')", (request_id,)).fetchone()[0])

    def _recompute_request_status(self, conn: sqlite3.Connection, request_id: int) -> None:
        row = conn.execute("""SELECT requests.data_mb,
            COALESCE(SUM(CASE WHEN schedules.status='received' THEN schedules.received_mb ELSE 0 END),0) AS got,
            SUM(CASE WHEN schedules.status='receiving' THEN 1 ELSE 0 END) AS recv,
            SUM(CASE WHEN schedules.status='scheduled' THEN 1 ELSE 0 END) AS sched
            FROM requests LEFT JOIN schedules ON schedules.request_id=requests.id WHERE requests.id=?""", (request_id,)).fetchone()
        if row["got"] + EPS >= float(row["data_mb"]): status = "received"
        elif row["recv"]: status = "receiving"
        elif row["sched"]: status = "scheduled"
        else:
            cur = conn.execute("SELECT status FROM requests WHERE id=?", (request_id,)).fetchone()["status"]
            status = "preempted" if cur == "preempted" else "pending"
        conn.execute("UPDATE requests SET status=? WHERE id=?", (status, request_id))

    def schedule_request(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        """安排一个或多个接力段；兼容单段 JSON。每站承担一段，余量由后续窗口接力。"""
        readonly(role)
        if role not in WRITE_ROLES: raise ApiError(403, "schedule_forbidden", "只有排程员可以安排接收")
        multi = isinstance(body.get("segments"), list)
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            if not request: raise ApiError(404, "request_not_found", "请求不存在")
            if request["status"] == "received": raise ApiError(409, "request_closed", "请求数据已全部接收，无需再排程")
            if request["status"] not in {"pending", "preempted", "scheduled", "receiving"}: raise ApiError(409, "request_closed", "请求当前不能排程")
            satellite = conn.execute("SELECT * FROM satellites WHERE id=?", (request["satellite_id"],)).fetchone()
            legs = self._parse_legs(conn, body)
            for i in range(1, len(legs)):
                for j in range(i):
                    if overlap(legs[i]["start"], legs[i]["end"], legs[j]["start"], legs[j]["end"]):
                        raise ApiError(409, "satellite_conflict", f"第 {i + 1} 段与第 {j + 1} 段时间重叠，同一卫星不能同时向两站下发")
            remaining = float(request["data_mb"]) - float(request["received_mb"]) - self._active_planned(conn, request_id)
            base_seq = int(conn.execute("SELECT COALESCE(MAX(seq),0) FROM schedules WHERE request_id=?", (request_id,)).fetchone()[0])
            new_capacity = 0.0
            quota_use: dict[tuple[str, str], int] = {}
            for i, leg in enumerate(legs):
                # 只排除本批尚未入库的其他新段；已入库接力段必须参与天线/同星占用检查
                self._validate_leg(conn, request, satellite, leg, set())
                leg["planned_mb"] = (leg["end"] - leg["start"]).total_seconds() * leg["rate"] / 8
                new_capacity += leg["planned_mb"]
                key = (request["tenant"], leg["window"]["station_id"], leg["start"].date().isoformat())
                quota_use[key] = quota_use.get(key, 0) + int((leg["end"] - leg["start"]).total_seconds())
            if remaining <= EPS: raise ApiError(409, "already_covered", "剩余数据已被现有接力段覆盖，无需再排程")
            if new_capacity + EPS < remaining:
                raise ApiError(409, "insufficient_capacity", "全部接力段可接收数据量仍不足，余下数据需要后续窗口", {"capacity_mb": new_capacity, "required_mb": remaining})
            for (tenant, station, day), seconds in quota_use.items():
                quota = conn.execute("SELECT daily_seconds FROM quotas WHERE tenant=? AND station_id=?", (tenant, station)).fetchone()
                used = self._used_quota(conn, tenant, station, day)
                if quota and used + seconds > quota["daily_seconds"]:
                    raise ApiError(409, "tenant_quota_exceeded", f"租户在地面站 {station} 当日配额不足", {"used_seconds": used, "requested_seconds": seconds, "limit": quota["daily_seconds"]})
            ids = [self._insert_segment(conn, request, leg, base_seq + i + 1, leg["planned_mb"], actor, "segment_scheduled") for i, leg in enumerate(legs)]
            self._recompute_request_status(conn, request_id)
            segments = [dict(conn.execute("SELECT * FROM schedules WHERE id=?", (sid,)).fetchone()) for sid in ids]
            if not multi: return segments[0]
            return {"request": dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()), "segments": segments}

    def get_schedule(self, schedule_id: int, role: str = "operator", tenant: str = "") -> dict[str, Any]:
        row = self.repo.conn.execute("""SELECT s.*,r.tenant,r.data_mb,r.received_mb AS request_received_mb,r.priority request_priority,r.deadline FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
        if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
        if role == "requester" and row["tenant"] != tenant: raise ApiError(403, "tenant_forbidden", "租户只能查看自己请求的接力段")
        data = dict(row)
        if role == "viewer": data = {k: data[k] for k in ("id", "request_id", "seq", "status", "starts_at", "ends_at", "station_id")}
        return data

    # ------------------------------------------------------------------ receive
    def transition(self, schedule_id: int, actor: str, role: str, tenant: str, target: str, body: dict[str, Any]) -> dict[str, Any]:
        readonly(role)
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.tenant,r.status request_status FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if target == "receiving":
                if role not in WRITE_ROLES: raise ApiError(403, "receive_forbidden", "当前角色不能开始接收")
                if row["status"] != "scheduled": raise ApiError(409, "invalid_transition", "只有已排程段可以开始接收")
                conn.execute("UPDATE schedules SET status='receiving',revision=revision+1,updated_at=? WHERE id=?", (iso(), schedule_id))
                conn.execute("UPDATE requests SET status='receiving' WHERE id=?", (row["request_id"],))
            elif target == "received":
                if role not in WRITE_ROLES: raise ApiError(403, "receive_forbidden", "当前角色不能完成接收")
                if row["status"] != "receiving": raise ApiError(409, "invalid_transition", "只有接收中段可以完成")
                actual = body.get("received_mb", row["planned_mb"])
                if not isinstance(actual, (int, float)) or actual < 0 or float(actual) > float(row["planned_mb"]) + EPS:
                    raise ApiError(400, "invalid_received_mb", "实收货量必须在 0 与本段计划货量之间", {"planned_mb": row["planned_mb"]})
                conn.execute("UPDATE schedules SET status='received',received_mb=?,revision=revision+1,updated_at=? WHERE id=?", (float(actual), iso(), schedule_id))
                conn.execute("UPDATE requests SET received_mb=received_mb+? WHERE id=?", (float(actual), row["request_id"]))
                # 数据已经收齐：后续排队段自动让位；未收完则由后续段继续接力
                done = conn.execute("SELECT r.data_mb,r.received_mb FROM requests r WHERE id=?", (row["request_id"],)).fetchone()
                if float(done["received_mb"]) + EPS >= float(done["data_mb"]):
                    conn.execute("UPDATE schedules SET status='canceled',disposition_reason='data_complete',revision=revision+1,updated_at=? WHERE request_id=? AND status='scheduled'", (iso(), row["request_id"]))
                self._recompute_request_status(conn, row["request_id"])
            else: raise ApiError(400, "invalid_transition", "未知状态")
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, f"receive_{target}", {"received_mb": body.get("received_mb")})
            return self.get_schedule(schedule_id)

    def cancel_schedule(self, schedule_id: int, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
        readonly(role)
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "取消原因必填")
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.tenant FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if role == "requester":
                if row["tenant"] != tenant: raise ApiError(403, "tenant_forbidden", "不能取消其他租户排程")
                if row["status"] != "scheduled": raise ApiError(409, "cancel_not_allowed", "接收开始后租户不能取消")
            elif role not in WRITE_ROLES: raise ApiError(403, "cancel_forbidden", "当前角色不能取消排程")
            if row["status"] == "received": raise ApiError(409, "received_data_protected", "已接收数据和天线占用不能取消或删除")
            if row["status"] == "canceled": return self.get_schedule(schedule_id)
            if row["status"] in {"preempted", "invalidated"}: raise ApiError(409, "invalid_transition", "已失效段无需取消")
            conn.execute("UPDATE schedules SET status='canceled',disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", (reason, iso(), schedule_id))
            self._recompute_request_status(conn, row["request_id"])
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "schedule_canceled", {"reason": reason})
            return self.get_schedule(schedule_id)

    # ------------------------------------------------------------------ preempt
    def emergency_preempt(self, schedule_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        readonly(role)
        if role != "commander": raise ApiError(403, "commander_required", "只有任务指挥官可以执行紧急抢占")
        order_id, reason = str(body.get("order_id", "")).strip(), str(body.get("reason", "")).strip()
        if not order_id or not reason: raise ApiError(400, "emergency_details_required", "order_id 和 reason 必填")
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.priority,r.tenant FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if row["status"] == "received": raise ApiError(409, "received_data_protected", "已接收数据的排程不能被抢占")
            if row["status"] not in {"scheduled", "receiving"}: raise ApiError(409, "invalid_transition", "当前排程不可抢占")
            conn.execute("UPDATE schedules SET status='preempted',disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", (f"{order_id}: {reason}", iso(), schedule_id))
            # 排队段与接收中段的未收数据都转到后续窗口；已收货量（received 段）不可抢占
            impacts = self._invalidate_and_replan(conn, [row], actor, role, f"emergency_{order_id}")
            self._recompute_request_status(conn, row["request_id"])
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "emergency_preemption", {"order_id": order_id, "reason": reason, "displaced_priority": row["priority"], "impacts": impacts})
            return {"schedule": self.get_schedule(schedule_id), "reschedule_required": not any(x["action"] == "replanned" for x in impacts), "order_id": order_id, "impacts": impacts}

    def priority_preempt(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        """指挥官插入紧急段：只挤掉优先级更低的排队段；接收中/已收/同级以上一律冲突并回传最新版次。"""
        readonly(role)
        if role != "commander": raise ApiError(403, "commander_required", "只有任务指挥官可以按优先级抢占")
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (body.get("request_id"),)).fetchone() if isinstance(body.get("request_id"), int) else None
            if not request: raise ApiError(404, "request_not_found", "紧急请求不存在")
            if request["status"] == "received": raise ApiError(409, "request_closed", "请求已收齐")
            satellite = conn.execute("SELECT * FROM satellites WHERE id=?", (request["satellite_id"],)).fetchone()
            legs = self._parse_legs(conn, body)
            if len(legs) != 1: raise ApiError(400, "single_segment_required", "优先级抢占一次插入一个紧急段")
            leg = legs[0]
            station = conn.execute("SELECT * FROM stations WHERE id=?", (leg["window"]["station_id"],)).fetchone()
            window, antenna, start, end, rate = leg["window"], leg["antenna"], leg["start"], leg["end"], leg["rate"]
            if request["satellite_id"] != window["satellite_id"] or window["station_id"] != antenna["station_id"]: raise ApiError(409, "window_mismatch", "卫星、窗口和天线不匹配")
            if satellite["status"] != "active" or station["status"] != "active" or antenna["status"] != "active": raise ApiError(409, "resource_inactive", "资源不可用")
            if station["weather"] != "clear": raise ApiError(409, "weather_blocked", "天气条件不允许接收")
            w_start, w_end = parse_time(window["starts_at"]), parse_time(window["ends_at"])
            if start < w_start or end > w_end: raise ApiError(409, "outside_visibility", "排程超出可见窗口")
            if end > parse_time(request["deadline"]): raise ApiError(409, "deadline_missed", "超过请求截止时间")
            max_rate = min(float(satellite["data_rate_mbps"]), float(window["max_rate_mbps"]), float(antenna["max_rate_mbps"]))
            if rate > max_rate: raise ApiError(409, "rate_exceeded", "速率超过上限", {"max_rate_mbps": max_rate})
            if self._maintenance(conn, station["id"], antenna["id"], iso(start), iso(end)): raise ApiError(409, "maintenance_conflict", "维护期不可抢占插入")
            urgent_priority = int(request["priority"])
            marks = "-1"
            blockers = conn.execute(f"""SELECT s.*,r.priority AS request_priority,r.tenant FROM schedules s JOIN requests r ON r.id=s.request_id
                                        WHERE s.status IN ('scheduled','receiving','received') AND s.starts_at<? AND s.ends_at>? AND s.id NOT IN ({marks})
                                        AND ((s.station_id=? AND s.antenna_id=?) OR s.satellite_id=?)""",
                                   (iso(end), iso(start), station["id"], antenna["id"], request["satellite_id"])).fetchall()
            victims: list[sqlite3.Row] = []
            for blocker in blockers:
                if blocker["status"] != "scheduled":
                    raise ApiError(409, "preempt_protected", "接收中或已收段受保护，不能抢占", self._conflict_detail(blocker))
                if int(blocker["request_priority"]) >= urgent_priority:
                    raise ApiError(409, "priority_conflict", "排队段优先级不低于紧急段，无法抢占（请查看最新版次）", self._conflict_detail(blocker))
                victims.append(blocker)
            leg["planned_mb"] = (end - start).total_seconds() * rate / 8
            remaining = float(request["data_mb"]) - float(request["received_mb"]) - self._active_planned(conn, request["id"])
            if remaining > EPS and leg["planned_mb"] + EPS < remaining:
                raise ApiError(409, "insufficient_capacity", "紧急段容量不足以覆盖剩余数据", {"capacity_mb": leg["planned_mb"], "required_mb": remaining})
            quota = conn.execute("SELECT daily_seconds FROM quotas WHERE tenant=? AND station_id=?", (request["tenant"], station["id"])).fetchone()
            used, seconds = self._used_quota(conn, request["tenant"], station["id"], start.date().isoformat()), int((end - start).total_seconds())
            if quota and used + seconds > quota["daily_seconds"]: raise ApiError(409, "tenant_quota_exceeded", "租户配额不足", {"used_seconds": used, "requested_seconds": seconds, "limit": quota["daily_seconds"]})
            # 先让紧急段落库占位，再重算被挤段，避免重算结果与紧急段重叠
            base_seq = int(conn.execute("SELECT COALESCE(MAX(seq),0) FROM schedules WHERE request_id=?", (request["id"],)).fetchone()[0])
            sid = self._insert_segment(conn, request, leg, base_seq + 1, leg["planned_mb"], actor, "priority_preempt_segment")
            impacts = self._invalidate_and_replan(conn, victims, actor, role, f"priority_order_p{urgent_priority}")
            self._recompute_request_status(conn, request["id"])
            Repository.audit(conn, request["id"], sid, actor, role, "priority_preempt", {"urgent_priority": urgent_priority, "displaced": [v["id"] for v in victims], "impacts": impacts})
            return {"schedule": self.get_schedule(sid), "displaced": [self._conflict_detail(v) for v in victims], "impacts": impacts}

    # ------------------------------------------------------------------ replan
    def _replan(self, conn: sqlite3.Connection, request: sqlite3.Row, needed_mb: float, exclude: set[int], after: datetime, actor: str, role: str, reason: str, skip_window_id: int | None = None, skip_station: str | None = None) -> tuple[list[int], float]:
        """把未收完的数据贪心转到后面的可用窗口，可跨站接力、可在窗口空隙内拆分。返回(新段id, 未覆盖MB)。"""
        satellite = conn.execute("SELECT * FROM satellites WHERE id=?", (request["satellite_id"],)).fetchone()
        deadline = parse_time(request["deadline"]); needed = float(needed_mb); created: list[int] = []
        if needed <= EPS: return created, 0.0
        windows = conn.execute("SELECT * FROM visibility_windows WHERE satellite_id=? AND ends_at>? ORDER BY starts_at,id", (request["satellite_id"], iso(after))).fetchall()
        windows = [w for w in windows if w["id"] != skip_window_id and w["station_id"] != skip_station]
        base_seq = int(conn.execute("SELECT COALESCE(MAX(seq),0) FROM schedules WHERE request_id=?", (request["id"],)).fetchone()[0])

        def gaps_of(window, station):
            avail_start = max(after, parse_time(window["starts_at"])); avail_end = parse_time(window["ends_at"])
            if avail_end <= avail_start: return []
            marks = ",".join("?" * len(exclude)) if exclude else "-1"
            busy = conn.execute(f"""SELECT starts_at,ends_at FROM schedules
                                    WHERE status IN ('scheduled','receiving','received') AND satellite_id=? AND starts_at<? AND ends_at>? AND id NOT IN ({marks})
                                    ORDER BY starts_at""", [request["satellite_id"], iso(avail_end), iso(avail_start)] + list(exclude)).fetchall()
            gaps, cursor = [], avail_start
            for b in busy:
                bs, be = parse_time(b["starts_at"]), parse_time(b["ends_at"])
                if bs > cursor: gaps.append((cursor, min(bs, avail_end)))
                cursor = max(cursor, be)
                if cursor >= avail_end: break
            if cursor < avail_end: gaps.append((cursor, avail_end))
            return gaps

        candidates: list[tuple[dict, tuple[datetime, datetime]]] = []
        for window in windows:
            station = conn.execute("SELECT * FROM stations WHERE id=?", (window["station_id"],)).fetchone()
            if not station or station["status"] != "active" or station["weather"] != "clear": continue
            for gap_s, gap_e in gaps_of(window, station):
                for antenna in conn.execute("SELECT * FROM antennas WHERE station_id=? AND status='active' ORDER BY max_rate_mbps DESC,id", (station["id"],)).fetchall():
                    candidates.append(({"window": window, "station": station, "antenna": antenna}, (gap_s, gap_e)))

        def try_place(whole: bool) -> bool:
            nonlocal needed, exclude
            for cand, (gap_s, gap_e) in list(candidates):
                if needed <= EPS: return True
                window, station, antenna = cand["window"], cand["station"], cand["antenna"]
                if self._maintenance(conn, station["id"], antenna["id"], iso(gap_s), iso(gap_e)): continue
                own = conn.execute("SELECT starts_at,ends_at FROM schedules WHERE station_id=? AND antenna_id=? AND status IN ('scheduled','receiving','received') AND starts_at<? AND ends_at>?", (station["id"], antenna["id"], iso(gap_e), iso(gap_s))).fetchall()
                sub_s, sub_e = gap_s, gap_e
                for b in own:
                    bs, be = parse_time(b["starts_at"]), parse_time(b["ends_at"])
                    if bs <= sub_s < be: sub_s = be
                    elif bs < sub_e: sub_e = bs
                if sub_e <= sub_s: continue
                rate = min(float(satellite["data_rate_mbps"]), float(window["max_rate_mbps"]), float(antenna["max_rate_mbps"]))
                want_seconds = needed * 8 / rate
                if whole and (sub_e - sub_s).total_seconds() + 1e-6 < want_seconds: continue
                seconds = min((sub_e - sub_s).total_seconds(), want_seconds)
                seg_end = sub_s + timedelta(seconds=seconds)
                if seg_end > deadline: continue
                quota = conn.execute("SELECT daily_seconds FROM quotas WHERE tenant=? AND station_id=?", (request["tenant"], station["id"])).fetchone()
                used = self._used_quota(conn, request["tenant"], station["id"], sub_s.date().isoformat(), exclude)
                if quota and used + int(seconds) > quota["daily_seconds"]: continue
                planned = seconds * rate / 8
                leg = {"window": window, "antenna": antenna, "start": sub_s, "end": seg_end, "rate": rate, "planned_mb": planned}
                sid = self._insert_segment(conn, request, leg, base_seq + len(created) + 1, planned, actor, f"replanned:{reason}")
                exclude = exclude | {sid}
                created.append(sid); needed -= planned
                return True
            return False

        # 第一轮：优先找能整段装下余量的最早窗口（跨站），避免在被抢占窗口尾部塞碎片
        while needed > EPS and try_place(whole=True): pass
        # 第二轮：只允许部分填充时再贪心拆分
        while needed > EPS and try_place(whole=False): pass
        return created, max(needed, 0.0)

    def _invalidate_and_replan(self, conn: sqlite3.Connection, victims: list[sqlite3.Row], actor: str, role: str, reason: str, skip_window_id: int | None = None, skip_station: str | None = None) -> list[dict[str, Any]]:
        """失效未开始段并在后续可用窗口重算；已收数据与接收中/已收段不受影响。"""
        impacts: list[dict[str, Any]] = []
        exclude = {v["id"] for v in victims}
        for v in victims:
            conn.execute("UPDATE schedules SET status='preempted',revision=revision+1,updated_at=? WHERE id=?", (iso(), v["id"]))
        for v in victims:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (v["request_id"],)).fetchone()
            # 从失效段原起点开始寻找后续窗口（当前时间之前的空隙会被忙时区间自然过滤）
            after_v = max(utcnow(), parse_time(v["starts_at"]))
            replacements, uncovered = self._replan(conn, request, v["planned_mb"], exclude, after_v, actor, role, reason, skip_window_id, skip_station)
            if replacements:
                conn.execute("UPDATE schedules SET status='invalidated',disposition_reason=?,supersedes_id=?,revision=revision+1,updated_at=? WHERE id=?",
                             (reason, replacements[0], iso(), v["id"]))
                for rid in replacements: conn.execute("UPDATE schedules SET supersedes_id=? WHERE id=?", (v["id"], rid))
                action = "replanned"
            else:
                action = "preempted"
            exclude |= set(replacements)
            impacts.append({"schedule_id": v["id"], "request_id": v["request_id"], "action": action, "reason": reason,
                            "old_start": v["starts_at"], "old_end": v["ends_at"], "planned_mb": v["planned_mb"],
                            "replacement_schedule_ids": replacements, "uncovered_mb": round(uncovered, 3)})
            self._recompute_request_status(conn, v["request_id"])
        return impacts

    def change_window(self, window_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        """窗口一变：已收/接收中保留，受影响未开始段失效并在后续窗口重算接力。"""
        readonly(role)
        if role not in WRITE_ROLES: raise ApiError(403, "window_forbidden", "当前角色不能变更可见窗口")
        start, end = parse_time(body.get("starts_at")), parse_time(body.get("ends_at"))
        if end <= start: raise ApiError(400, "invalid_window", "窗口结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
            if not window: raise ApiError(404, "window_not_found", "可见窗口不存在")
            rows = conn.execute("SELECT * FROM schedules WHERE window_id=? AND status IN ('scheduled','receiving','received') ORDER BY starts_at,id", (window_id,)).fetchall()
            impacts: list[dict[str, Any]] = []; victims: list[sqlite3.Row] = []
            for row in rows:
                invalid = parse_time(row["starts_at"]) < start or parse_time(row["ends_at"]) > end
                if row["status"] == "received":
                    impacts.append({"schedule_id": row["id"], "request_id": row["request_id"], "action": "preserve_received_data", "reason": "已接收数据不可回滚", "invalid": invalid})
                elif row["status"] == "receiving":
                    impacts.append({"schedule_id": row["id"], "request_id": row["request_id"], "action": "preserve_in_flight", "reason": "接收中段保留，仅未开始段参与重算", "invalid": invalid})
                elif invalid:
                    victims.append(row)
                else:
                    impacts.append({"schedule_id": row["id"], "request_id": row["request_id"], "action": "unchanged", "reason": "新窗口仍覆盖该段"})
            # 受影响请求在其他窗口的已收/接收中段同样只做保留报告
            victim_requests = {v["request_id"] for v in victims}
            for rid in victim_requests:
                for row in conn.execute("SELECT * FROM schedules WHERE request_id=? AND status IN ('received','receiving') ORDER BY id", (rid,)).fetchall():
                    if row["window_id"] == window_id: continue
                    impacts.append({"schedule_id": row["id"], "request_id": rid,
                                    "action": "preserve_received_data" if row["status"] == "received" else "preserve_in_flight",
                                    "reason": "已收数据/接收中段不受窗口变化影响", "invalid": False})
            impacts[0:0] = self._invalidate_and_replan(conn, victims, actor, role, "visibility_window_changed", skip_window_id=window_id)
            conn.execute("UPDATE visibility_windows SET starts_at=?,ends_at=?,revision=revision+1 WHERE id=?", (iso(start), iso(end), window_id))
            Repository.audit(conn, None, None, actor, role, "visibility_window_changed", {"window_id": window_id, "impacts": impacts})
            return {"window": dict(conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()), "impacts": impacts}

    def reschedule(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        readonly(role)
        if role not in WRITE_ROLES: raise ApiError(403, "schedule_forbidden", "只有排程员可以重排")
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            if not request: raise ApiError(404, "request_not_found", "请求不存在")
            if request["status"] != "preempted": raise ApiError(409, "reschedule_not_needed", "只有被抢占请求需要重排")
            conn.execute("UPDATE requests SET status='pending' WHERE id=?", (request_id,))
            Repository.audit(conn, request_id, None, actor, role, "reschedule_requested", {"reason": body.get("reason", "")})
            return dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone())

    # ------------------------------------------------------------------ queries
    def state(self, role: str, tenant: str) -> dict[str, Any]:
        conn = self.repo.conn
        if role == "requester":
            requests_ = [dict(r) for r in conn.execute("SELECT * FROM requests WHERE tenant=? ORDER BY id DESC", (tenant,))]
            schedules = [dict(r) for r in conn.execute("SELECT s.* FROM schedules s JOIN requests r ON r.id=s.request_id WHERE r.tenant=? ORDER BY s.request_id,s.seq", (tenant,))]
            stations, windows = [], []
        elif role == "viewer":
            requests_, stations, windows = [], [], []
            schedules = [dict(r) for r in conn.execute("SELECT id,request_id,seq,status,starts_at,ends_at,station_id FROM schedules ORDER BY id DESC")]
        else:
            requests_ = [dict(r) for r in conn.execute("SELECT * FROM requests ORDER BY id DESC")]
            schedules = [dict(r) for r in conn.execute("SELECT * FROM schedules ORDER BY request_id,seq,id")]
            stations = [dict(r) for r in conn.execute("SELECT * FROM stations ORDER BY id")]
            windows = [dict(r) for r in conn.execute("SELECT * FROM visibility_windows ORDER BY id")]
        return {"requests": requests_, "segments": schedules, "schedules": schedules, "stations": stations, "windows": windows, "server_time": iso()}

    def audit_log(self, role: str, limit: int = 200) -> dict[str, Any]:
        if role not in WRITE_ROLES and role != "auditor": raise ApiError(403, "audit_forbidden", "审计日志仅排程角色与审计角色可查")
        rows = self.repo.conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (min(max(limit, 1), 1000),)).fetchall()
        return {"entries": [dict(r) for r in rows]}


def send_json(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode(); handler.send_response(status); handler.send_header("Content-Type", "application/json; charset=utf-8"); handler.send_header("Content-Length", str(len(raw))); handler.end_headers(); handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    service: SatelliteSchedulingService; web_root: Path
    def log_message(self, fmt: str, *args: Any) -> None: print(f"{self.address_string()} - {fmt % args}")
    def body(self) -> dict[str, Any]:
        size = int(self.headers.get("Content-Length", "0"))
        if not size: return {}
        try: value = json.loads(self.rfile.read(size))
        except json.JSONDecodeError as exc: raise ApiError(400, "invalid_json", "请求体不是有效 JSON") from exc
        if not isinstance(value, dict): raise ApiError(400, "invalid_json", "请求体必须是对象")
        return value
    def get_api(self, path: str) -> tuple[int, Any]:
        if path == "/health": return 200, {"status": "ok", "service": "satellite-scheduling"}
        actor, role, tenant = self.service.identity(self.headers)
        if path == "/api/state": return 200, self.service.state(role, tenant)
        if path == "/api/audit-log": return 200, self.service.audit_log(role)
        parts = [p for p in path.split("/") if p]
        if len(parts) == 3 and parts[:2] == ["api", "schedules"] and parts[2].isdigit(): return 200, self.service.get_schedule(int(parts[2]), role, tenant)
        raise ApiError(404, "not_found", "接口不存在")
    def post_api(self, path: str) -> tuple[int, Any]:
        actor, role, tenant = self.service.identity(self.headers)
        if role == "auditor": raise ApiError(403, "read_only", "审计角色只能查看，不能修改")
        body = self.body(); parts = [p for p in path.split("/") if p]
        actions = {
            "/api/satellites": lambda: (201, self.service.create_satellite(actor, role, body)),
            "/api/stations": lambda: (201, self.service.create_station(actor, role, body)),
            "/api/antennas": lambda: (201, self.service.create_antenna(actor, role, body)),
            "/api/maintenance": lambda: (201, self.service.create_maintenance(actor, role, body)),
            "/api/visibility-windows": lambda: (201, self.service.create_window(actor, role, body)),
            "/api/quotas": lambda: (200, self.service.set_quota(actor, role, body)),
            "/api/requests": lambda: (201, self.service.create_request(actor, role, tenant, body)),
            "/api/commands/priority-preempt": lambda: (200, self.service.priority_preempt(actor, role, body)),
        }
        if path in actions: return actions[path]()
        if len(parts) == 4 and parts[:2] == ["api", "stations"] and parts[3] == "weather": return 200, self.service.set_station_weather(parts[2], actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "requests"] and parts[2].isdigit():
            rid, action = int(parts[2]), parts[3]
            if action == "schedule": return 201, self.service.schedule_request(rid, actor, role, body)
            if action == "reschedule": return 200, self.service.reschedule(rid, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "schedules"] and parts[2].isdigit():
            sid, action = int(parts[2]), parts[3]
            if action in {"start", "complete"}: return 200, self.service.transition(sid, actor, role, tenant, "receiving" if action == "start" else "received", body)
            if action == "cancel": return 200, self.service.cancel_schedule(sid, actor, role, tenant, body)
            if action == "preempt": return 200, self.service.emergency_preempt(sid, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "visibility-windows"] and parts[2].isdigit() and parts[3] == "change": return 200, self.service.change_window(int(parts[2]), actor, role, body)
        raise ApiError(404, "not_found", "接口不存在")
    def handle_request(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
            status, payload = self.get_api(parsed.path) if method == "GET" else self.post_api(parsed.path); send_json(self, status, payload)
        except ApiError as exc:
            payload = {"error": exc.code, "message": exc.message}
            if exc.details is not None: payload["details"] = exc.details
            send_json(self, exc.status, payload)
        except Exception as exc: print(f"unhandled error: {exc!r}"); send_json(self, 500, {"error": "internal_error", "message": str(exc)})
    def do_GET(self) -> None: self.handle_request("GET")
    def do_POST(self) -> None: self.handle_request("POST")


def create_server(db_path: str | Path, host: str = "127.0.0.1", port: int = PORT) -> ThreadingHTTPServer:
    service = SatelliteSchedulingService(db_path); handler = type("SatelliteHandler", (Handler,), {"service": service, "web_root": Path(__file__).resolve().parent / "static"}); return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--host", default="127.0.0.1"); parser.add_argument("--port", type=int, default=PORT); parser.add_argument("--db", default=os.environ.get("SAT_DB", "satellite_scheduling.db")); args = parser.parse_args()
    server = create_server(args.db, args.host, args.port); print(f"satellite-scheduling listening on http://{args.host}:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__ == "__main__": main()
