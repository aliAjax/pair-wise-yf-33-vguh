#!/usr/bin/env python3
"""Multi-station satellite scheduling service using only Python's standard library."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

PORT = 8204
ROLES = {"viewer", "requester", "operator", "commander", "auditor"}


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


class Repository:
    def __init__(self, path: str | Path):
        self.conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row; self.conn.execute("PRAGMA foreign_keys=ON"); self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS satellites(id TEXT PRIMARY KEY, name TEXT NOT NULL, data_rate_mbps REAL NOT NULL, priority INTEGER NOT NULL, storage_capacity_mb REAL NOT NULL, tenant TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE IF NOT EXISTS stations(id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', weather TEXT NOT NULL DEFAULT 'clear');
        CREATE TABLE IF NOT EXISTS antennas(id TEXT PRIMARY KEY, station_id TEXT NOT NULL REFERENCES stations(id), max_rate_mbps REAL NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE IF NOT EXISTS maintenance(id INTEGER PRIMARY KEY AUTOINCREMENT, station_id TEXT NOT NULL REFERENCES stations(id), antenna_id TEXT REFERENCES antennas(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, reason TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS visibility_windows(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT NOT NULL REFERENCES satellites(id), station_id TEXT NOT NULL REFERENCES stations(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, max_rate_mbps REAL NOT NULL, revision INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT NOT NULL REFERENCES satellites(id), tenant TEXT NOT NULL, priority INTEGER NOT NULL, data_mb REAL NOT NULL, deadline TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_by TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS quotas(id INTEGER PRIMARY KEY AUTOINCREMENT, tenant TEXT NOT NULL, station_id TEXT NOT NULL REFERENCES stations(id), daily_seconds INTEGER NOT NULL, UNIQUE(tenant,station_id));
        CREATE TABLE IF NOT EXISTS schedules(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER NOT NULL REFERENCES requests(id), segment_no INTEGER NOT NULL DEFAULT 1, window_id INTEGER NOT NULL REFERENCES visibility_windows(id), station_id TEXT NOT NULL REFERENCES stations(id), antenna_id TEXT NOT NULL REFERENCES antennas(id), satellite_id TEXT NOT NULL REFERENCES satellites(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, rate_mbps REAL NOT NULL, data_mb REAL, status TEXT NOT NULL DEFAULT 'scheduled', revision INTEGER NOT NULL DEFAULT 1, disposition_reason TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_schedules_request ON schedules(request_id);
        CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER, schedule_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
        """)
        self._migrate_segments()

    def _migrate_segments(self) -> None:
        """Upgrade a legacy single-schedule-per-request database to multi-segment relay schema."""
        conn = self.conn
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(schedules)")]
        if not cols: return
        indexes = conn.execute("PRAGMA index_list(schedules)").fetchall()
        has_unique = any(bool(r["unique"]) for r in indexes)
        if has_unique:
            conn.execute("ALTER TABLE schedules RENAME TO schedules_old")
            conn.execute("""CREATE TABLE schedules(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL REFERENCES requests(id),
                segment_no INTEGER NOT NULL DEFAULT 1,
                window_id INTEGER NOT NULL REFERENCES visibility_windows(id),
                station_id TEXT NOT NULL REFERENCES stations(id),
                antenna_id TEXT NOT NULL REFERENCES antennas(id),
                satellite_id TEXT NOT NULL REFERENCES satellites(id),
                starts_at TEXT NOT NULL,
                ends_at TEXT NOT NULL,
                rate_mbps REAL NOT NULL,
                data_mb REAL,
                status TEXT NOT NULL DEFAULT 'scheduled',
                revision INTEGER NOT NULL DEFAULT 1,
                disposition_reason TEXT,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL)""")
            conn.execute("""INSERT INTO schedules(id,request_id,segment_no,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,data_mb,status,revision,disposition_reason,created_by,created_at,updated_at)
                            SELECT id,request_id,1,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,
                                   (julianday(ends_at)-julianday(starts_at))*86400*rate_mbps/8,
                                   status,revision,disposition_reason,created_by,created_at,updated_at FROM schedules_old""")
            conn.execute("DROP TABLE schedules_old")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schedules_request ON schedules(request_id)")
        else:
            if "segment_no" not in cols:
                conn.execute("ALTER TABLE schedules ADD COLUMN segment_no INTEGER NOT NULL DEFAULT 1")
            if "data_mb" not in cols:
                conn.execute("ALTER TABLE schedules ADD COLUMN data_mb REAL")
                conn.execute("UPDATE schedules SET data_mb=(julianday(ends_at)-julianday(starts_at))*86400*rate_mbps/8 WHERE data_mb IS NULL")

    @contextmanager
    def tx(self):
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

    def create_satellite(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护卫星")
        sid, name, tenant = str(body.get("id", "")).strip(), str(body.get("name", "")).strip(), str(body.get("tenant", "")).strip()
        rate, priority, capacity = body.get("data_rate_mbps"), body.get("priority"), body.get("storage_capacity_mb")
        if not sid or not name or not tenant or not isinstance(rate, (int, float)) or float(rate) <= 0 or not isinstance(priority, int) or not 1 <= priority <= 10 or not isinstance(capacity, (int, float)) or float(capacity) <= 0:
            raise ApiError(400, "invalid_satellite", "卫星参数不完整")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO satellites(id,name,data_rate_mbps,priority,storage_capacity_mb,tenant,status) VALUES(?,?,?,?,?,?,?)", (sid, name, float(rate), priority, float(capacity), tenant, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM satellites WHERE id=?", (sid,)).fetchone())

    def create_station(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护地面站")
        sid, name = str(body.get("id", "")).strip(), str(body.get("name", "")).strip()
        weather = str(body.get("weather", "clear")).lower()
        if not sid or not name or weather not in {"clear", "rain", "storm", "closed"}: raise ApiError(400, "invalid_station", "地面站名称或天气状态无效")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO stations(id,name,status,weather) VALUES(?,?,?,?)", (sid, name, body.get("status", "active"), weather))
            return dict(conn.execute("SELECT * FROM stations WHERE id=?", (sid,)).fetchone())

    def create_antenna(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护天线")
        aid, station, rate = str(body.get("id", "")).strip(), str(body.get("station_id", "")).strip(), body.get("max_rate_mbps")
        if not aid or not station or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_antenna", "天线参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("INSERT OR REPLACE INTO antennas(id,station_id,max_rate_mbps,status) VALUES(?,?,?,?)", (aid, station, float(rate), body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM antennas WHERE id=?", (aid,)).fetchone())

    def create_maintenance(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "maintenance_forbidden", "当前角色不能登记维护")
        station, start, end, reason = str(body.get("station_id", "")).strip(), parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), str(body.get("reason", "")).strip()
        antenna = body.get("antenna_id")
        if not station or end <= start or not reason: raise ApiError(400, "invalid_maintenance", "维护参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            if antenna and not conn.execute("SELECT 1 FROM antennas WHERE id=? AND station_id=?", (antenna, station)).fetchone(): raise ApiError(400, "antenna_station_mismatch", "天线不属于该站")
            cur = conn.execute("INSERT INTO maintenance(station_id,antenna_id,starts_at,ends_at,reason) VALUES(?,?,?,?,?)", (station, antenna, iso(start), iso(end), reason))
            return dict(conn.execute("SELECT * FROM maintenance WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_window(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "window_forbidden", "当前角色不能维护可见窗口")
        satellite, station, start, end, rate = str(body.get("satellite_id", "")).strip(), str(body.get("station_id", "")).strip(), parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), body.get("max_rate_mbps")
        if not satellite or not station or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_window", "可见窗口参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM satellites WHERE id=?", (satellite,)).fetchone(): raise ApiError(404, "satellite_not_found", "卫星不存在")
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            cur = conn.execute("INSERT INTO visibility_windows(satellite_id,station_id,starts_at,ends_at,max_rate_mbps) VALUES(?,?,?,?,?)", (satellite, station, iso(start), iso(end), float(rate)))
            return dict(conn.execute("SELECT * FROM visibility_windows WHERE id=?", (cur.lastrowid,)).fetchone())

    def set_quota(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "quota_forbidden", "当前角色不能设置租户配额")
        tenant, station, seconds = str(body.get("tenant", "")).strip(), str(body.get("station_id", "")).strip(), body.get("daily_seconds")
        if not tenant or not station or not isinstance(seconds, int) or seconds <= 0: raise ApiError(400, "invalid_quota", "配额参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("INSERT INTO quotas(tenant,station_id,daily_seconds) VALUES(?,?,?) ON CONFLICT(tenant,station_id) DO UPDATE SET daily_seconds=excluded.daily_seconds", (tenant, station, seconds))
            return dict(conn.execute("SELECT * FROM quotas WHERE tenant=? AND station_id=?", (tenant, station)).fetchone())

    def create_request(self, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
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

    def _used_quota(self, conn: sqlite3.Connection, tenant: str, station: str, day: str, exclude_schedule: int | None = None) -> int:
        sql = """SELECT COALESCE(SUM((julianday(s.ends_at)-julianday(s.starts_at))*86400),0) FROM schedules s JOIN requests r ON r.id=s.request_id
                 WHERE r.tenant=? AND s.station_id=? AND substr(s.starts_at,1,10)=? AND s.status IN ('scheduled','receiving','received')"""
        args: list[Any] = [tenant, station, day]
        if exclude_schedule is not None: sql += " AND s.id!=?"; args.append(exclude_schedule)
        return int(conn.execute(sql, args).fetchone()[0])

    @staticmethod
    def _schedule_version(conn: sqlite3.Connection) -> int:
        return int(conn.execute("SELECT COALESCE(MAX(revision),0) FROM schedules").fetchone()[0])

    def _check_if_match(self, conn: sqlite3.Connection, body: dict[str, Any]) -> None:
        """Optimistic concurrency: a scheduler may submit against the revision they last read."""
        if_match = body.get("if_match")
        if if_match is None: return
        if not isinstance(if_match, int) or isinstance(if_match, bool): raise ApiError(400, "invalid_if_match", "if_match 必须是整数版本号")
        current = self._schedule_version(conn)
        if if_match != current:
            raise ApiError(409, "revision_conflict", "提交所依据的版次已过期，请以最新版次为准", {"latest_revision": current, "your_revision": if_match})

    def _next_segment_no(self, conn: sqlite3.Connection, request_id: int) -> int:
        return int(conn.execute("SELECT COALESCE(MAX(segment_no),0)+1 FROM schedules WHERE request_id=?", (request_id,)).fetchone()[0])

    def _received_total(self, conn: sqlite3.Connection, request_id: int) -> float:
        return float(conn.execute("SELECT COALESCE(SUM(data_mb),0) FROM schedules WHERE request_id=? AND status='received'", (request_id,)).fetchone()[0])

    def _update_request_status(self, conn: sqlite3.Connection, request_id: int) -> str:
        request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
        if not request: return "pending"
        data_mb = float(request["data_mb"])
        received = float(conn.execute("SELECT COALESCE(SUM(data_mb),0) FROM schedules WHERE request_id=? AND status='received'", (request_id,)).fetchone()[0])
        planned = float(conn.execute("SELECT COALESCE(SUM(data_mb),0) FROM schedules WHERE request_id=? AND status IN ('scheduled','receiving','received')", (request_id,)).fetchone()[0])
        if received >= data_mb - 1e-9: status = "received"
        elif received > 1e-9: status = "relaying"
        elif planned >= data_mb - 1e-9: status = "scheduled"
        else: status = "pending"
        conn.execute("UPDATE requests SET status=? WHERE id=?", (status, request_id))
        return status

    def _plan_segment(self, conn: sqlite3.Connection, request: sqlite3.Row, window_id: int, antenna_id: str,
                      start: datetime, end: datetime, rate: float, data_mb: float) -> dict[str, Any]:
        """Validate a candidate segment and return its insert fields; raises ApiError on any conflict."""
        window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
        antenna = conn.execute("SELECT * FROM antennas WHERE id=?", (antenna_id,)).fetchone()
        if not window or not antenna: raise ApiError(404, "schedule_ref_not_found", "窗口或天线不存在")
        if request["satellite_id"] != window["satellite_id"] or window["station_id"] != antenna["station_id"]:
            raise ApiError(409, "window_mismatch", "卫星、窗口和天线不匹配")
        station = conn.execute("SELECT * FROM stations WHERE id=?", (window["station_id"],)).fetchone()
        satellite = conn.execute("SELECT * FROM satellites WHERE id=?", (request["satellite_id"],)).fetchone()
        if satellite["status"] != "active" or station["status"] != "active" or antenna["status"] != "active": raise ApiError(409, "resource_inactive", "卫星、地面站或天线不可用")
        if station["weather"] != "clear": raise ApiError(409, "weather_blocked", "天气条件不允许接收")
        w_start, w_end = parse_time(window["starts_at"]), parse_time(window["ends_at"])
        if start < w_start or end > w_end: raise ApiError(409, "outside_visibility", "排程超出可见窗口")
        if end > parse_time(request["deadline"]): raise ApiError(409, "deadline_missed", "预计结束时间超过请求截止时间")
        max_rate = min(float(satellite["data_rate_mbps"]), float(window["max_rate_mbps"]), float(antenna["max_rate_mbps"]))
        if float(rate) > max_rate: raise ApiError(409, "rate_exceeded", "请求速率超过可用上限", {"max_rate_mbps": max_rate})
        capacity = (end - start).total_seconds() * float(rate) / 8
        if capacity < float(data_mb) - 1e-9: raise ApiError(409, "insufficient_capacity", "窗口内可接收数据量不足", {"capacity_mb": capacity, "required_mb": float(data_mb)})
        maintenance = conn.execute("""SELECT * FROM maintenance WHERE station_id=? AND (antenna_id IS NULL OR antenna_id=?) AND starts_at<? AND ends_at>?""", (station["id"], antenna_id, iso(end), iso(start))).fetchone()
        if maintenance: raise ApiError(409, "maintenance_conflict", "天线或地面站处于维护期", dict(maintenance))
        equipment = conn.execute("SELECT id FROM schedules WHERE station_id=? AND antenna_id=? AND starts_at<? AND ends_at>? AND status IN ('scheduled','receiving')", (station["id"], antenna_id, iso(end), iso(start))).fetchone()
        if equipment: raise ApiError(409, "antenna_conflict", "天线时段已被占用", {"schedule_id": equipment["id"], "latest_revision": self._schedule_version(conn)})
        satellite_conflict = conn.execute("SELECT id FROM schedules WHERE satellite_id=? AND starts_at<? AND ends_at>? AND status IN ('scheduled','receiving')", (request["satellite_id"], iso(end), iso(start))).fetchone()
        if satellite_conflict: raise ApiError(409, "satellite_conflict", "同一卫星时段已被其他站接收", {"schedule_id": satellite_conflict["id"], "latest_revision": self._schedule_version(conn)})
        quota = conn.execute("SELECT daily_seconds FROM quotas WHERE tenant=? AND station_id=?", (request["tenant"], station["id"])).fetchone()
        used = self._used_quota(conn, request["tenant"], station["id"], start.date().isoformat())
        duration = int((end - start).total_seconds())
        if quota and used + duration > quota["daily_seconds"]: raise ApiError(409, "tenant_quota_exceeded", "租户当日地面站配额不足", {"used_seconds": used, "requested_seconds": duration, "limit": quota["daily_seconds"]})
        return {"window_id": window_id, "station_id": station["id"], "antenna_id": antenna_id,
                "satellite_id": request["satellite_id"], "starts_at": iso(start), "ends_at": iso(end),
                "rate_mbps": float(rate), "data_mb": float(data_mb)}

    def _insert_segment(self, conn: sqlite3.Connection, request: sqlite3.Row, planned: dict[str, Any], actor: str) -> int:
        segment_no = self._next_segment_no(conn, int(request["id"]))
        cur = conn.execute("""INSERT INTO schedules(request_id,segment_no,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,data_mb,created_by,created_at,updated_at)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                           (int(request["id"]), segment_no, planned["window_id"], planned["station_id"], planned["antenna_id"], planned["satellite_id"],
                            planned["starts_at"], planned["ends_at"], planned["rate_mbps"], planned["data_mb"], actor, iso(), iso()))
        return cur.lastrowid

    def _allocate_remaining(self, conn: sqlite3.Connection, request: sqlite3.Row, target_mb: float, actor: str, role: str,
                            exclude_window_id: int | None = None) -> list[dict[str, Any]]:
        """Best-effort relay: place up to target_mb into later clear-weather windows at other antennas."""
        target = float(target_mb)
        if target <= 1e-9: return []
        satellite = conn.execute("SELECT * FROM satellites WHERE id=?", (request["satellite_id"],)).fetchone()
        if not satellite or satellite["status"] != "active": return []
        now = utcnow()
        windows = conn.execute("""SELECT w.* FROM visibility_windows w JOIN stations st ON st.id=w.station_id
                                  WHERE w.satellite_id=? AND st.status='active' AND st.weather='clear'
                                    AND w.ends_at<=? AND w.starts_at>=? AND (? IS NULL OR w.id<>?)
                                  ORDER BY w.starts_at""",
                               (request["satellite_id"], request["deadline"], iso(now), exclude_window_id, exclude_window_id)).fetchall()
        created: list[dict[str, Any]] = []
        remaining = target
        for w in windows:
            if remaining <= 1e-9: break
            antennas = conn.execute("SELECT * FROM antennas WHERE station_id=? AND status='active' ORDER BY max_rate_mbps DESC", (w["station_id"],)).fetchall()
            for ant in antennas:
                rate = min(float(satellite["data_rate_mbps"]), float(w["max_rate_mbps"]), float(ant["max_rate_mbps"]))
                w_start, w_end = parse_time(w["starts_at"]), parse_time(w["ends_at"])
                seg_end = w_start + timedelta(seconds=remaining * 8.0 / rate)
                if seg_end > w_end: seg_end = w_end
                if seg_end <= w_start: continue
                data_mb = (seg_end - w_start).total_seconds() * rate / 8.0
                if data_mb <= 0: continue
                if conn.execute("SELECT 1 FROM schedules WHERE station_id=? AND antenna_id=? AND starts_at<? AND ends_at>? AND status IN ('scheduled','receiving')",
                               (w["station_id"], ant["id"], iso(seg_end), iso(w_start))).fetchone(): continue
                if conn.execute("SELECT 1 FROM schedules WHERE satellite_id=? AND starts_at<? AND ends_at>? AND status IN ('scheduled','receiving')",
                               (request["satellite_id"], iso(seg_end), iso(w_start))).fetchone(): continue
                quota = conn.execute("SELECT daily_seconds FROM quotas WHERE tenant=? AND station_id=?", (request["tenant"], w["station_id"])).fetchone()
                used = self._used_quota(conn, request["tenant"], w["station_id"], w_start.date().isoformat())
                duration = int((seg_end - w_start).total_seconds())
                if quota and used + duration > quota["daily_seconds"]: continue
                planned = {"window_id": w["id"], "station_id": w["station_id"], "antenna_id": ant["id"],
                           "satellite_id": request["satellite_id"], "starts_at": iso(w_start), "ends_at": iso(seg_end),
                           "rate_mbps": rate, "data_mb": data_mb}
                sid = self._insert_segment(conn, request, planned, actor)
                created.append(self.get_schedule(sid))
                remaining -= data_mb
                break
        return created

    def schedule_request(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "schedule_forbidden", "只有排程员可以安排接收")
        window_id, antenna_id = body.get("window_id"), str(body.get("antenna_id", "")).strip()
        start, end, rate = parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), body.get("rate_mbps")
        if not isinstance(window_id, int) or not antenna_id or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_schedule", "排程参数无效")
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            if not request: raise ApiError(404, "request_not_found", "请求不存在")
            if request["status"] not in {"pending", "preempted", "relaying"}: raise ApiError(409, "request_closed", "请求当前不能排程")
            self._check_if_match(conn, body)
            planned = self._plan_segment(conn, request, window_id, antenna_id, start, end, float(rate), float(request["data_mb"]))
            schedule_id = self._insert_segment(conn, request, planned, actor)
            self._update_request_status(conn, request_id)
            Repository.audit(conn, request_id, schedule_id, actor, role, "schedule_created",
                             {"window_id": window_id, "antenna_id": antenna_id, "capacity_mb": planned["data_mb"], "segment_no": self._next_segment_no(conn, request_id) - 1})
            return self.get_schedule(schedule_id)

    def relay_request(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        """Split a request across multiple ground-station segments, or auto-allocate the remainder to later windows."""
        if role not in {"operator", "commander"}: raise ApiError(403, "schedule_forbidden", "只有排程员可以安排接力接收")
        segments_in, auto = body.get("segments"), body.get("auto")
        if not segments_in and not auto: raise ApiError(400, "relay_params_required", "需要提供 segments 或 auto")
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            if not request: raise ApiError(404, "request_not_found", "请求不存在")
            if request["status"] not in {"pending", "preempted", "relaying"}: raise ApiError(409, "request_closed", "请求当前不能排程")
            self._check_if_match(conn, body)
            remaining = float(request["data_mb"]) - self._received_total(conn, request_id)
            if remaining <= 1e-9: raise ApiError(409, "already_complete", "请求已全部接收")
            created: list[dict[str, Any]] = []
            if isinstance(segments_in, list) and segments_in:
                total = 0.0
                for seg in segments_in:
                    if not isinstance(seg, dict): raise ApiError(400, "invalid_relay_segment", "接力段必须是对象")
                    w_id, ant_id = seg.get("window_id"), str(seg.get("antenna_id", "")).strip()
                    s, e, r, mb = parse_time(seg.get("starts_at")), parse_time(seg.get("ends_at")), seg.get("rate_mbps"), seg.get("data_mb")
                    if not isinstance(w_id, int) or not ant_id or e <= s or not isinstance(r, (int, float)) or float(r) <= 0 or not isinstance(mb, (int, float)) or float(mb) <= 0:
                        raise ApiError(400, "invalid_relay_segment", "接力段参数无效")
                    total += float(mb)
                if total > remaining + 1e-9: raise ApiError(409, "relay_exceeds_remaining", "接力段数据量超过剩余未收数据", {"remaining_mb": remaining, "planned_mb": total})
                for seg in segments_in:
                    planned = self._plan_segment(conn, request, int(seg["window_id"]), str(seg["antenna_id"]).strip(),
                                                 parse_time(seg["starts_at"]), parse_time(seg["ends_at"]),
                                                 float(seg["rate_mbps"]), float(seg["data_mb"]))
                    sid = self._insert_segment(conn, request, planned, actor)
                    created.append(self.get_schedule(sid))
            if auto:
                mb = body.get("data_mb")
                target = remaining if not isinstance(mb, (int, float)) or float(mb) <= 0 else min(float(mb), remaining)
                created.extend(self._allocate_remaining(conn, request, target, actor, role, exclude_window_id=body.get("exclude_window_id") if isinstance(body.get("exclude_window_id"), int) else None))
            status = self._update_request_status(conn, request_id)
            Repository.audit(conn, request_id, None, actor, role, "relay_created", {"segments": [s["id"] for s in created]})
            planned_total = sum(float(s["data_mb"]) for s in created if s["status"] in ("scheduled", "receiving"))
            remaining_mb = float(request["data_mb"]) - self._received_total(conn, request_id) - planned_total
            return {"request": dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()),
                    "segments": created, "request_status": status, "remaining_mb": remaining_mb}

    def get_schedule(self, schedule_id: int, role: str | None = None, tenant: str | None = None) -> dict[str, Any]:
        row = self.repo.conn.execute("""SELECT s.*,r.tenant,r.data_mb,r.priority request_priority,r.deadline FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
        if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
        if role == "requester" and tenant and row["tenant"] != tenant:
            raise ApiError(404, "schedule_not_found", "排程不存在")
        return dict(row)

    def transition(self, schedule_id: int, actor: str, role: str, tenant: str, target: str, body: dict[str, Any]) -> dict[str, Any]:
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.tenant,r.status request_status FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if target == "receiving":
                if role not in {"operator", "commander"}: raise ApiError(403, "receive_forbidden", "当前角色不能开始接收")
                if row["status"] != "scheduled": raise ApiError(409, "invalid_transition", "只有已排程任务可以开始接收")
                conn.execute("UPDATE schedules SET status='receiving',revision=revision+1,updated_at=? WHERE id=?", (iso(), schedule_id))
            elif target == "received":
                if role not in {"operator", "commander"}: raise ApiError(403, "receive_forbidden", "当前角色不能完成接收")
                if row["status"] != "receiving": raise ApiError(409, "invalid_transition", "只有接收中任务可以完成")
                conn.execute("UPDATE schedules SET status='received',revision=revision+1,updated_at=? WHERE id=?", (iso(), schedule_id))
                self._update_request_status(conn, row["request_id"])
            else: raise ApiError(400, "invalid_transition", "未知状态")
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, f"receive_{target}", {})
            return self.get_schedule(schedule_id)

    def cancel_schedule(self, schedule_id: int, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "取消原因必填")
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.tenant FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if role == "requester":
                if row["tenant"] != tenant: raise ApiError(403, "tenant_forbidden", "不能取消其他租户排程")
                if row["status"] != "scheduled": raise ApiError(409, "cancel_not_allowed", "接收开始后租户不能取消")
            elif role not in {"operator", "commander"}: raise ApiError(403, "cancel_forbidden", "当前角色不能取消排程")
            if row["status"] == "received": raise ApiError(409, "received_data_protected", "已接收数据不能取消或删除")
            if row["status"] == "canceled": return self.get_schedule(schedule_id)
            conn.execute("UPDATE schedules SET status='canceled',disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", (reason, iso(), schedule_id))
            self._update_request_status(conn, row["request_id"])
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "schedule_canceled", {"reason": reason})
            return self.get_schedule(schedule_id)

    def _disrupt_segment(self, conn: sqlite3.Connection, row: sqlite3.Row, disposition: str, effective_end: datetime | None = None) -> tuple[float, str]:
        """Invalidate a scheduled/receiving segment; preserve received data and antenna occupancy.

        Returns (received_mb, action). A receiving segment keeps the portion already received;
        ``effective_end`` caps the reception (new window end for a schedule change, else now).
        """
        if row["status"] == "receiving":
            eff = effective_end or min(parse_time(row["ends_at"]), utcnow())
            actual_seconds = max(0.0, (eff - parse_time(row["starts_at"])).total_seconds())
            received_mb = min(float(row["data_mb"] or 0), actual_seconds * float(row["rate_mbps"]) / 8.0)
            conn.execute("UPDATE schedules SET status='received',data_mb=?,ends_at=?,disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?",
                         (received_mb, iso(eff), disposition, iso(), row["id"]))
            return received_mb, "relay_handover"
        conn.execute("UPDATE schedules SET status='preempted',disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?",
                     (disposition, iso(), row["id"]))
        return 0.0, "preempted"

    def _relay_remaining(self, conn: sqlite3.Connection, request_row: sqlite3.Row, actor: str, role: str,
                         exclude_window_id: int | None = None) -> tuple[list[dict[str, Any]], float]:
        remaining = float(request_row["data_mb"]) - self._received_total(conn, int(request_row["id"]))
        if remaining <= 1e-9: return [], 0.0
        return self._allocate_remaining(conn, request_row, remaining, actor, role, exclude_window_id=exclude_window_id), remaining

    def emergency_preempt(self, schedule_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "commander": raise ApiError(403, "commander_required", "只有任务指挥官可以执行紧急抢占")
        order_id, reason = str(body.get("order_id", "")).strip(), str(body.get("reason", "")).strip()
        if not order_id or not reason: raise ApiError(400, "emergency_details_required", "order_id 和 reason 必填")
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.priority,r.tenant,r.data_mb request_data_mb FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if row["status"] == "received": raise ApiError(409, "received_data_protected", "已接收数据的排程不能被抢占")
            if row["status"] not in {"scheduled", "receiving"}: raise ApiError(409, "invalid_transition", "当前排程不可抢占")
            preempt_priority = body.get("priority")
            benefiting_request_id = body.get("request_id")
            if isinstance(benefiting_request_id, int):
                br = conn.execute("SELECT priority FROM requests WHERE id=?", (benefiting_request_id,)).fetchone()
                if not br: raise ApiError(404, "benefiting_request_not_found", "受益请求不存在")
                preempt_priority = br["priority"]
            if not isinstance(preempt_priority, int) or isinstance(preempt_priority, bool) or not 1 <= preempt_priority <= 10:
                raise ApiError(400, "priority_required", "抢占必须提供 priority 或有效的 request_id（优先级 1-10）")
            if int(row["priority"]) >= preempt_priority:
                raise ApiError(409, "priority_too_high", "不能抢占优先级不高于己方的排队段", {"target_priority": row["priority"], "your_priority": preempt_priority})
            received_mb, action = self._disrupt_segment(conn, row, f"{order_id}: {reason}")
            request_row = conn.execute("SELECT * FROM requests WHERE id=?", (row["request_id"],)).fetchone()
            relay_segments, _ = self._relay_remaining(conn, request_row, actor, role, exclude_window_id=row["window_id"])
            self._update_request_status(conn, row["request_id"])
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "emergency_preemption",
                             {"order_id": order_id, "reason": reason, "displaced_priority": row["priority"],
                              "your_priority": preempt_priority, "received_mb": received_mb,
                              "relay_segments": [s["id"] for s in relay_segments]})
            return {"schedule": self.get_schedule(schedule_id), "reschedule_required": True, "order_id": order_id,
                    "action": action, "received_mb": received_mb, "relay_segments": relay_segments}

    def change_window(self, window_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "window_forbidden", "当前角色不能变更可见窗口")
        start, end = parse_time(body.get("starts_at")), parse_time(body.get("ends_at"))
        if end <= start: raise ApiError(400, "invalid_window", "窗口结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
            if not window: raise ApiError(404, "window_not_found", "可见窗口不存在")
            rows = conn.execute("SELECT * FROM schedules WHERE window_id=? AND status IN ('scheduled','receiving','received')", (window_id,)).fetchall()
            impacts = []
            for row in rows:
                sched_start, sched_end = parse_time(row["starts_at"]), parse_time(row["ends_at"])
                invalid = sched_start < start or sched_end > end
                if row["status"] == "received":
                    impacts.append({"schedule_id": row["id"], "action": "preserve_received_data", "reason": "已接收数据不可回滚", "invalid": invalid})
                    continue
                if not invalid:
                    impacts.append({"schedule_id": row["id"], "action": "unchanged", "reason": "新窗口仍覆盖排程"})
                    continue
                received_mb, action = self._disrupt_segment(conn, row, "visibility_window_changed", effective_end=min(sched_end, end))
                request_row = conn.execute("SELECT * FROM requests WHERE id=?", (row["request_id"],)).fetchone()
                relay_segments, remaining = self._relay_remaining(conn, request_row, actor, role, exclude_window_id=window_id)
                self._update_request_status(conn, row["request_id"])
                impacts.append({"schedule_id": row["id"], "request_id": row["request_id"], "action": action,
                                "reason": "新窗口无法覆盖原排程，未收数据已重排到后续可用窗口",
                                "received_mb": received_mb, "remaining_mb": remaining,
                                "relay_segments": [s["id"] for s in relay_segments],
                                "old_start": row["starts_at"], "old_end": row["ends_at"]})
            conn.execute("UPDATE visibility_windows SET starts_at=?,ends_at=?,revision=revision+1 WHERE id=?", (iso(start), iso(end), window_id))
            Repository.audit(conn, None, None, actor, role, "visibility_window_changed", {"window_id": window_id, "impacts": impacts})
            return {"window": dict(conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()), "impacts": impacts}

    def set_weather(self, station_id: str, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "weather_forbidden", "当前角色不能变更天气")
        weather = str(body.get("weather", "")).strip().lower()
        if weather not in {"clear", "rain", "storm", "closed"}: raise ApiError(400, "invalid_weather", "天气状态无效")
        with self.repo.tx() as conn:
            station = conn.execute("SELECT * FROM stations WHERE id=?", (station_id,)).fetchone()
            if not station: raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("UPDATE stations SET weather=? WHERE id=?", (weather, station_id))
            impacts = []
            if weather != "clear":
                rows = conn.execute("SELECT * FROM schedules WHERE station_id=? AND status IN ('scheduled','receiving')", (station_id,)).fetchall()
                for row in rows:
                    received_mb, action = self._disrupt_segment(conn, row, f"weather:{weather}")
                    request_row = conn.execute("SELECT * FROM requests WHERE id=?", (row["request_id"],)).fetchone()
                    relay_segments, remaining = self._relay_remaining(conn, request_row, actor, role)
                    self._update_request_status(conn, row["request_id"])
                    impacts.append({"schedule_id": row["id"], "request_id": row["request_id"], "action": action,
                                    "reason": "天气转坏，未收数据已重排到其他可用窗口",
                                    "received_mb": received_mb, "remaining_mb": remaining,
                                    "relay_segments": [s["id"] for s in relay_segments]})
            Repository.audit(conn, None, station_id, actor, role, "station_weather_changed", {"station_id": station_id, "weather": weather, "impacts": impacts})
            return {"station": dict(conn.execute("SELECT * FROM stations WHERE id=?", (station_id,)).fetchone()), "impacts": impacts}

    def reschedule(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role == "auditor": raise ApiError(403, "auditor_readonly", "审计角色只能查看，不能修改")
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            if not request: raise ApiError(404, "request_not_found", "请求不存在")
            if request["status"] not in {"preempted", "relaying"}: raise ApiError(409, "reschedule_not_needed", "当前请求不需要重排")
            conn.execute("UPDATE requests SET status='pending' WHERE id=?", (request_id,))
            Repository.audit(conn, request_id, None, actor, role, "reschedule_requested", {"reason": body.get("reason", "")})
            return dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone())

    def state(self, role: str, tenant: str) -> dict[str, Any]:
        conn = self.repo.conn
        if role == "requester":
            requests = [dict(r) for r in conn.execute("SELECT * FROM requests WHERE tenant=? ORDER BY id DESC", (tenant,))]
            schedules = [dict(r) for r in conn.execute("SELECT s.* FROM schedules s JOIN requests r ON r.id=s.request_id WHERE r.tenant=? ORDER BY s.id DESC", (tenant,))]
            stations = []
        elif role == "viewer":
            requests, schedules, stations = [], [dict(r) for r in conn.execute("SELECT id,status,starts_at,ends_at FROM schedules ORDER BY id DESC")], []
        else:  # operator, commander, auditor: read-only over everything auditor may view
            requests = [dict(r) for r in conn.execute("SELECT * FROM requests ORDER BY id DESC")]
            schedules = [dict(r) for r in conn.execute("SELECT * FROM schedules ORDER BY id DESC")]
            stations = [dict(r) for r in conn.execute("SELECT * FROM stations ORDER BY id")]
        return {"requests": requests, "schedules": schedules, "stations": stations, "server_time": iso()}


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
        parts = [p for p in path.split("/") if p]
        if len(parts) == 3 and parts[:2] == ["api", "schedules"] and parts[2].isdigit(): return 200, self.service.get_schedule(int(parts[2]), role, tenant)
        raise ApiError(404, "not_found", "接口不存在")
    def post_api(self, path: str) -> tuple[int, Any]:
        actor, role, tenant = self.service.identity(self.headers); body = self.body(); parts = [p for p in path.split("/") if p]
        if role == "auditor": raise ApiError(403, "auditor_readonly", "审计角色只能查看，不能修改")
        actions = {
            "/api/satellites": lambda: (201, self.service.create_satellite(actor, role, body)),
            "/api/stations": lambda: (201, self.service.create_station(actor, role, body)),
            "/api/antennas": lambda: (201, self.service.create_antenna(actor, role, body)),
            "/api/maintenance": lambda: (201, self.service.create_maintenance(actor, role, body)),
            "/api/visibility-windows": lambda: (201, self.service.create_window(actor, role, body)),
            "/api/quotas": lambda: (200, self.service.set_quota(actor, role, body)),
            "/api/requests": lambda: (201, self.service.create_request(actor, role, tenant, body)),
        }
        if path in actions: return actions[path]()
        if len(parts) == 4 and parts[:2] == ["api", "requests"] and parts[2].isdigit():
            rid, action = int(parts[2]), parts[3]
            if action == "schedule": return 201, self.service.schedule_request(rid, actor, role, body)
            if action == "relay": return 201, self.service.relay_request(rid, actor, role, body)
            if action == "reschedule": return 200, self.service.reschedule(rid, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "schedules"] and parts[2].isdigit():
            sid, action = int(parts[2]), parts[3]
            if action in {"start", "complete"}: return 200, self.service.transition(sid, actor, role, tenant, "receiving" if action == "start" else "received", body)
            if action == "cancel": return 200, self.service.cancel_schedule(sid, actor, role, tenant, body)
            if action == "preempt": return 200, self.service.emergency_preempt(sid, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "visibility-windows"] and parts[2].isdigit() and parts[3] == "change": return 200, self.service.change_window(int(parts[2]), actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "stations"] and parts[3] == "weather": return 200, self.service.set_weather(parts[2], actor, role, body)
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
