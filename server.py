#!/usr/bin/env python3
"""
Mission Control Dashboard — read-only monitoring for the Hermes multi-agent system.

Architecture:
  - ThreadingHTTPServer on 127.0.0.1:51764
  - GET /            → serves index.html
  - GET /api/snapshot → full JSON snapshot of all telemetry
  - GET /events      → Server-Sent Events pushing /api/snapshot every 5s
  - GET /api/board   → list personal operator tasks
  - POST /api/board  → create a personal task
  - POST /api/board/update?id=  → update task fields
  - POST /api/board/delete?id=  → delete a task by id

Data sources (all read-only via SQLite URI mode=ro + PRAGMA query_only=1):
  - /root/.hermes/state.db         — sessions, messages, token usage
  - /root/.hermes/agent-logs.db    — agent activity log
  - /root/.hermes/gateway_state.json — live gateway status
  - /proc/stat, /proc/meminfo      — VPS health
  - /etc/crontab, /etc/cron.d/*    — cron jobs
  - board.db (project-local)        — personal operator task board (read-write)

Python stdlib only. No pip, no npm.
"""

import json
import os
import sqlite3
import time
import uuid
import threading
from datetime import datetime, timezone, timedelta
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs

# ─── Constants ────────────────────────────────────────────────────────────────

HERMES_HOME = "/root/.hermes"
STATE_DB = f"{HERMES_HOME}/state.db"
AGENT_LOGS_DB = f"{HERMES_HOME}/agent-logs.db"
GATEWAY_STATE = f"{HERMES_HOME}/gateway_state.json"
BOARD_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "board.db")

# Sami's timezone: UTC+3 — all dashboard time display uses this
TZ_SAMI = timezone(timedelta(hours=3))

HOST = "127.0.0.1"
PORT = 51764
SSE_INTERVAL = 5  # seconds between SSE pushes

# ─── Read-only SQLite helper ──────────────────────────────────────────────────

def ro_connect(path):
    """Open a read-only SQLite connection using URI mode=ro + query_only pragma."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=1")
    conn.row_factory = sqlite3.Row
    return conn

# ─── Data Function 1: gateway_data() ──────────────────────────────────────────

def gateway_data():
    """Read gateway_state.json and return gateway state, platform statuses,
    active agent count, and uptime."""
    try:
        with open(GATEWAY_STATE, "r") as f:
            gw = json.load(f)

        # Uptime: start_time is clock ticks since boot (from /proc/stat)
        # We need btime to convert to epoch
        uptime_str = "unknown"
        uptime_secs = 0
        try:
            btime = None
            with open("/proc/stat", "r") as sf:
                for line in sf:
                    if line.startswith("btime"):
                        btime = int(line.split()[1])
                        break
            if btime is not None:
                ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
                start_epoch = btime + (gw.get("start_time", 0) / ticks)
                uptime_secs = max(0, time.time() - start_epoch)
                hrs = int(uptime_secs // 3600)
                mins = int((uptime_secs % 3600) // 60)
                uptime_str = f"{hrs}h {mins}m"
        except Exception:
            pass

        platforms = {}
        for pname, pinfo in (gw.get("platforms") or {}).items():
            platforms[pname] = {
                "state": pinfo.get("state", "unknown"),
                "error_code": pinfo.get("error_code"),
                "error_message": pinfo.get("error_message"),
                "updated_at": pinfo.get("updated_at"),
            }

        return {
            "gateway_state": gw.get("gateway_state", "unknown"),
            "pid": gw.get("pid"),
            "active_agents": gw.get("active_agents", 0),
            "uptime": uptime_str,
            "uptime_seconds": round(uptime_secs, 0),
            "updated_at": gw.get("updated_at"),
            "restart_requested": gw.get("restart_requested", False),
            "exit_reason": gw.get("exit_reason"),
            "platforms": platforms,
        }
    except Exception as e:
        return {"error": f"gateway_data: {e}", "gateway_state": "error"}


# ─── Data Function 2: activity_data() ─────────────────────────────────────────

def activity_data():
    """Query agent-logs.db for the last 50 entries, per-agent stats, overall
    totals, and a 7-day daily breakdown.

    NOTE: The agent-logs.db schema has model_used and status columns swapped
    in the actual data (model_used contains status values like 'completed',
    and status contains model names like 'glm-5.2'). We swap them back here.
    """
    try:
        conn = ro_connect(AGENT_LOGS_DB)
        cur = conn.cursor()

        # Last 50 entries — swap model_used/status columns back to correct meaning
        # Sort by created_at DESC, id DESC
        cur.execute("""
            SELECT id, agent_name, task_description,
                   status   AS model_used,
                   model_used AS status,
                   created_at
            FROM agent_logs
            ORDER BY created_at DESC, id DESC
            LIMIT 50
        """)
        recent = [dict(r) for r in cur.fetchall()]

        # Per-agent stats
        cur.execute("""
            SELECT agent_name,
                   COUNT(*) as total,
                   SUM(CASE WHEN model_used = 'completed' THEN 1 ELSE 0 END) as completed,
                   SUM(CASE WHEN model_used = 'failed' THEN 1 ELSE 0 END) as failed,
                   MAX(created_at) as last_seen
            FROM agent_logs
            GROUP BY agent_name
            ORDER BY agent_name
        """)
        per_agent_raw = [dict(r) for r in cur.fetchall()]

        # Get last task + model per agent
        # Also build per-agent 7-day daily activity
        # Use UTC+3 for day bucketing so "today" matches Sami's local date
        now = datetime.now(TZ_SAMI)
        day_keys = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

        per_agent = []
        for a in per_agent_raw:
            cur.execute("""
                SELECT task_description, status AS model
                FROM agent_logs
                WHERE agent_name = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, (a["agent_name"],))
            last = cur.fetchone()

            # Per-agent daily counts
            cur.execute("""
                SELECT created_at FROM agent_logs
                WHERE agent_name = ? AND created_at >= ?
            """, (a["agent_name"], (now - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")))
            agent_daily = {dk: 0 for dk in day_keys}
            for row in cur.fetchall():
                ts = row["created_at"]
                try:
                    # Parse UTC timestamp and convert to UTC+3 for correct day bucketing
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    day = dt.astimezone(TZ_SAMI).strftime("%Y-%m-%d")
                    if day in agent_daily:
                        agent_daily[day] += 1
                except Exception:
                    pass
            agent_daily_list = [agent_daily[dk] for dk in sorted(day_keys)]

            per_agent.append({
                "agent_name": a["agent_name"],
                "total": a["total"],
                "completed": a["completed"] or 0,
                "failed": a["failed"] or 0,
                "last_task": last["task_description"] if last else None,
                "last_seen": a["last_seen"],
                "model": last["model"] if last else None,
                "daily": agent_daily_list,
            })

        # Overall totals
        cur.execute("SELECT COUNT(*) as n FROM agent_logs")
        total_tasks = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(DISTINCT agent_name) as n FROM agent_logs")
        total_agents = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) as n FROM agent_logs WHERE model_used = 'completed'")
        total_completed = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) as n FROM agent_logs WHERE model_used = 'failed'")
        total_failed = cur.fetchone()["n"]

        # 7-day daily breakdown
        seven_days_ago = now - timedelta(days=7)
        cur.execute("""
            SELECT created_at FROM agent_logs
            WHERE created_at >= ?
            ORDER BY created_at DESC
        """, (seven_days_ago.strftime("%Y-%m-%dT%H:%M:%S"),))

        # Build day buckets
        daily = {}
        for i in range(7):
            d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            daily[d] = 0
        for row in cur.fetchall():
            ts = row["created_at"]
            try:
                # Parse UTC timestamp and convert to UTC+3 for correct day bucketing
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                day = dt.astimezone(TZ_SAMI).strftime("%Y-%m-%d")
                if day in daily:
                    daily[day] += 1
            except Exception:
                pass
        daily_list = [{"date": k, "count": v} for k, v in sorted(daily.items())]

        conn.close()

        return {
            "recent": recent,
            "activity": recent,  # alias for frontend
            "per_agent": per_agent,
            "agents": per_agent,  # alias for frontend
            "totals": {
                "total": total_tasks,
                "tasks": total_tasks,
                "agents": total_agents,
                "completed": total_completed,
                "failed": total_failed,
            },
            "stats": {
                "total": total_tasks,
                "completed": total_completed,
                "failed": total_failed,
            },
            "daily": daily_list,
            "activity_by_day": [{"date": d["date"], "total": d["count"]} for d in daily_list],
        }
    except Exception as e:
        return {"error": f"activity_data: {e}"}


# ─── Data Function 3: sessions_data() ─────────────────────────────────────────

def sessions_data():
    """Query state.db for session count, message count, token totals, and the
    25 most recent sessions. Timestamps are Unix float seconds — passed as-is."""
    try:
        conn = ro_connect(STATE_DB)
        cur = conn.cursor()

        # Session count
        cur.execute("SELECT COUNT(*) as n FROM sessions")
        session_count = cur.fetchone()["n"]

        # Message count
        cur.execute("SELECT COUNT(*) as n FROM messages")
        message_count = cur.fetchone()["n"]

        # Token totals
        cur.execute("""
            SELECT
                COALESCE(SUM(input_tokens), 0) as input_tokens,
                COALESCE(SUM(output_tokens), 0) as output_tokens,
                COALESCE(SUM(cache_read_tokens), 0) as cache_read_tokens,
                COALESCE(SUM(cache_write_tokens), 0) as cache_write_tokens,
                COALESCE(SUM(reasoning_tokens), 0) as reasoning_tokens,
                COALESCE(SUM(estimated_cost_usd), 0) as estimated_cost_usd,
                COALESCE(SUM(actual_cost_usd), 0) as actual_cost_usd
            FROM sessions
        """)
        tokens = dict(cur.fetchone())

        # 25 most recent sessions
        cur.execute("""
            SELECT id, source, chat_id, chat_type, model, title,
                   started_at, ended_at, message_count, tool_call_count,
                   input_tokens, output_tokens, cache_read_tokens,
                   cache_write_tokens, reasoning_tokens,
                   estimated_cost_usd, actual_cost_usd,
                   handoff_state, archived, api_call_count
            FROM sessions
            ORDER BY started_at DESC
            LIMIT 25
        """)
        recent_sessions = [dict(r) for r in cur.fetchall()]

        conn.close()

        return {
            "session_count": session_count,
            "count": session_count,  # alias for frontend
            "message_count": message_count,
            "totals": {
                "messages": message_count,
                "input_tokens": tokens.get("input_tokens", 0),
                "output_tokens": tokens.get("output_tokens", 0),
                "cache_read_tokens": tokens.get("cache_read_tokens", 0),
                "cache_write_tokens": tokens.get("cache_write_tokens", 0),
                "reasoning_tokens": tokens.get("reasoning_tokens", 0),
                "estimated_cost_usd": tokens.get("estimated_cost_usd", 0),
                "actual_cost_usd": tokens.get("actual_cost_usd", 0),
            },
            "token_totals": tokens,
            "recent_sessions": recent_sessions,
        }
    except Exception as e:
        return {"error": f"sessions_data: {e}"}


# ─── Data Function 4: vps_health() ────────────────────────────────────────────

def vps_health():
    """CPU from two /proc/stat samples, RAM from /proc/meminfo, disk from
    os.statvfs. No subprocess calls."""
    try:
        # --- CPU: two samples 0.1s apart ---
        def read_cpu():
            with open("/proc/stat", "r") as f:
                for line in f:
                    if line.startswith("cpu "):
                        parts = line.split()
                        # user, nice, system, idle, iowait, irq, softirq, steal
                        return [int(x) for x in parts[1:]]
            return None

        cpu1 = read_cpu()
        time.sleep(0.1)
        cpu2 = read_cpu()

        cpu_percent = 0.0
        if cpu1 and cpu2:
            total1 = sum(cpu1)
            total2 = sum(cpu2)
            # idle is index 3
            idle1 = cpu1[3] if len(cpu1) > 3 else 0
            idle2 = cpu2[3] if len(cpu2) > 3 else 0
            total_diff = total2 - total1
            idle_diff = idle2 - idle1
            if total_diff > 0:
                cpu_percent = round((1 - idle_diff / total_diff) * 100, 1)

        # --- RAM from /proc/meminfo ---
        meminfo = {}
        with open("/proc/meminfo", "r") as f:
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    key = parts[0].strip()
                    val = int(parts[1].strip().split()[0])  # in kB
                    meminfo[key] = val

        mem_total = meminfo.get("MemTotal", 0) * 1024  # bytes
        mem_available = meminfo.get("MemAvailable", 0) * 1024
        mem_used = mem_total - mem_available
        mem_percent = round((mem_used / mem_total * 100), 1) if mem_total > 0 else 0

        # --- Disk from os.statvfs ---
        st = os.statvfs("/")
        disk_total = st.f_blocks * st.f_frsize
        disk_free = st.f_bavail * st.f_frsize
        disk_used = disk_total - disk_free
        disk_percent = round((disk_used / disk_total * 100), 1) if disk_total > 0 else 0

        # --- Load average ---
        load1, load5, load15 = os.getloadavg()

        # --- Uptime (system) ---
        with open("/proc/uptime", "r") as f:
            system_uptime_secs = float(f.read().split()[0])
        sys_up_hrs = int(system_uptime_secs // 3600)
        sys_up_mins = int((system_uptime_secs % 3600) // 60)

        # --- DB size ---
        db_size_bytes = 0
        try:
            for dbfile in [STATE_DB, AGENT_LOGS_DB, f"{HERMES_HOME}/kanban.db"]:
                if os.path.exists(dbfile):
                    db_size_bytes += os.path.getsize(dbfile)
        except Exception:
            pass
        db_size_mb = round(db_size_bytes / (1024 * 1024), 1)

        return {
            "cpu_pct": cpu_percent,
            "cpu_percent": cpu_percent,
            "mem_pct": mem_percent,
            "mem_used_mb": round(mem_used / (1024 * 1024), 0),
            "mem_total_mb": round(mem_total / (1024 * 1024), 0),
            "disk_pct": disk_percent,
            "disk_used_gb": round(disk_used / (1024 * 1024 * 1024), 1),
            "disk_total_gb": round(disk_total / (1024 * 1024 * 1024), 1),
            "db_size_mb": db_size_mb,
            "load_avg": {"1": round(load1, 2), "5": round(load5, 2), "15": round(load15, 2)},
            "memory": {
                "total_bytes": mem_total,
                "used_bytes": mem_used,
                "available_bytes": mem_available,
                "percent": mem_percent,
            },
            "disk": {
                "total_bytes": disk_total,
                "used_bytes": disk_used,
                "free_bytes": disk_free,
                "percent": disk_percent,
            },
            "system_uptime": f"{sys_up_hrs}h {sys_up_mins}m",
            "system_uptime_seconds": round(system_uptime_secs, 0),
        }
    except Exception as e:
        return {"error": f"vps_health: {e}"}


# ─── Data Function 5: cron_jobs() ─────────────────────────────────────────────

# Minimal cron field parsing for human-readable descriptions
_CRON_NAMES = {
    "month": {"jan": "1", "feb": "2", "mar": "3", "apr": "4", "may": "5", "jun": "6",
              "jul": "7", "aug": "8", "sep": "9", "oct": "10", "nov": "11", "dec": "12"},
    "dow": {"sun": "0", "mon": "1", "tue": "2", "wed": "3", "thu": "4", "fri": "5", "sat": "6"},
}


def _cron_to_english(fields):
    """Convert 5 cron fields (min, hour, dom, month, dow) to plain English."""
    minute, hour, dom, month, dow = fields

    # Handle step values (*/N)
    if minute == "0" and hour == "0" and dom == "*" and month == "*" and dow == "*":
        return "Midnight daily"
    if minute == "0" and hour != "*" and dom == "*" and month == "*" and dow == "*":
        return f"Daily at {int(hour):02d}:00"
    if minute != "*" and minute != "0" and hour != "*" and dom == "*" and month == "*" and dow == "*":
        return f"Daily at {int(hour):02d}:{int(minute):02d}"
    if minute == "*" and hour == "*" and dom == "*" and month == "*" and dow == "*":
        return "Every minute"
    if minute.startswith("*/"):
        step = minute[2:]
        return f"Every {step} minutes"
    if minute == "*" and hour == "*":
        return f"Every minute"
    if hour == "*" and minute != "*":
        return f"Every hour at minute {minute}"
    if dom == "*" and month == "*" and dow != "*":
        # Day of week specific
        dow_names = {"0": "Sunday", "7": "Sunday", "1": "Monday", "2": "Tuesday",
                     "3": "Wednesday", "4": "Thursday", "5": "Friday", "6": "Saturday"}
        dow_lower = dow.lower()
        for name, val in _CRON_NAMES["dow"].items():
            dow_lower = dow_lower.replace(name, val)
        parts = []
        for d in dow_lower.split(","):
            parts.append(dow_names.get(d.strip(), d.strip()))
        dow_str = ", ".join(parts)
        if minute == "0" and hour != "*":
            return f"Every {dow_str} at {int(hour):02d}:00"
        return f"Every {dow_str} at {int(hour):02d}:{int(minute):02d}"
    if dom == "1" and month == "*" and dow == "*" and minute == "0" and hour == "0":
        return "1st of every month at midnight"
    if dom != "*" and dom != "1" and month == "*" and dow == "*" and minute == "0" and hour != "*":
        return f"Monthly on day {dom} at {int(hour):02d}:00"

    # Fallback: show raw
    return f"{minute} {hour} {dom} {month} {dow}"


def _cron_to_english_tz(fields, tz):
    """Like _cron_to_english but converts the cron hour from UTC to the given timezone.

    Only handles the common cases (daily at HH:MM, midnight, etc.).
    Falls back to _cron_to_english for complex schedules."""
    if len(fields) < 5:
        return _cron_to_english(fields)

    minute, hour, dom, month, dow = fields

    # Only convert when hour is a specific number (not *, */N, ranges, or lists)
    try:
        h_utc = int(hour)
    except (ValueError, TypeError):
        return _cron_to_english(fields)

    # Calculate the UTC+3 hour
    offset = tz.utcoffset(None)
    offset_hours = int(offset.total_seconds() // 3600) if offset else 0
    h_tz = (h_utc + offset_hours) % 24

    # Rebuild with converted hour
    tz_fields = [minute, str(h_tz), dom, month, dow]

    # Use the same logic as _cron_to_english
    if minute == "0" and h_utc == 0 and dom == "*" and month == "*" and dow == "*":
        return f"Daily at {h_tz:02d}:00 (UTC+3)"
    if minute == "0" and dom == "*" and month == "*" and dow == "*":
        return f"Daily at {h_tz:02d}:00 (UTC+3)"
    if minute != "*" and minute != "0" and dom == "*" and month == "*" and dow == "*":
        return f"Daily at {h_tz:02d}:{int(minute):02d} (UTC+3)"
    if dom == "*" and month == "*" and dow != "*":
        dow_names = {"0": "Sunday", "7": "Sunday", "1": "Monday", "2": "Tuesday",
                     "3": "Wednesday", "4": "Thursday", "5": "Friday", "6": "Saturday"}
        dow_lower = dow.lower()
        for name, val in _CRON_NAMES["dow"].items():
            dow_lower = dow_lower.replace(name, val)
        parts_list = []
        for d in dow_lower.split(","):
            parts_list.append(dow_names.get(d.strip(), d.strip()))
        dow_str = ", ".join(parts_list)
        if minute == "0":
            return f"Every {dow_str} at {h_tz:02d}:00 (UTC+3)"
        return f"Every {dow_str} at {h_tz:02d}:{int(minute):02d} (UTC+3)"
    if dom == "1" and month == "*" and dow == "*" and minute == "0" and h_utc == 0:
        return f"1st of every month at {h_tz:02d}:00 (UTC+3)"
    if dom != "*" and dom != "1" and month == "*" and dow == "*" and minute == "0":
        return f"Monthly on day {dom} at {h_tz:02d}:00 (UTC+3)"

    # Fallback: show raw with tz note
    return f"{minute} {hour} {dom} {month} {dow} (UTC, {h_tz:02d}:{minute if minute != '*' else '00'} UTC+3)"


def cron_jobs():
    """Read cron files, strip the username field in system files, label each
    job 'hermes' or 'system', and convert schedule to plain English."""
    try:
        jobs = []

        def parse_line(line, source_file, has_username):
            """Parse a single cron line into a job dict."""
            line = line.strip()
            if not line or line.startswith("#"):
                return None

            # Skip env assignments (SHELL=, PATH=, etc.)
            parts = line.split(None, 5)
            if len(parts) < (6 if has_username else 5):
                return None
            if "=" in parts[0] and not parts[0].startswith("@"):
                return None

            if has_username:
                # fields: min hour dom month dow user command
                fields = line.split(None, 6)
                if len(fields) < 7:
                    return None
                minute, hour, dom, month, dow, username, command = fields[:7]
            else:
                # fields: min hour dom month dow command
                fields = line.split(None, 5)
                if len(fields) < 6:
                    return None
                minute, hour, dom, month, dow, command = fields[:6]
                username = "root"

            schedule = f"{{minute}} {{hour}} {{dom}} {{month}} {{dow}}".format(minute=minute, hour=hour, dom=dom, month=month, dow=dow)
            english = _cron_to_english([minute, hour, dom, month, dow])

            # Label hermes vs system
            is_hermes = any(kw in command.lower() for kw in
                            ["hermes", "/root/.hermes", "agent", "cleanup-logs"])
            label = "hermes" if is_hermes else "system"
            category = label  # alias for frontend

            # Short name: extract a meaningful name from the command
            cmd_parts = command.split()
            name = "unknown"
            if cmd_parts:
                # Look for the most descriptive part of the command
                for part in cmd_parts:
                    pl = part.lower()
                    if any(kw in pl for kw in ["cleanup-logs", "run-parts", "anacron",
                                               "e2scrub", "debian-sa1", "sysstat",
                                               "cron.hourly", "cron.daily", "cron.weekly",
                                               "cron.monthly"]):
                        name = part.split("/")[-1].replace(".sh", "").replace(".py", "")
                        break
                if name == "unknown":
                    # Use the source file + first meaningful command word
                    first = cmd_parts[0]
                    if "/" in first:
                        name = first.split("/")[-1].replace(".sh", "").replace(".py", "")
                    else:
                        name = first
                if "run-parts" in command:
                    for part in cmd_parts:
                        if "cron." in part:
                            name = part.split("/")[-1].rstrip(";")  # e.g., "cron.hourly"
                            break

            return {
                "schedule": schedule,
                "english": english,
                "detail": english,
                "name": name,
                "user": username,
                "command": command,
                "source": source_file,
                "label": label,
                "category": category,
            }

        # 1. User crontab: /var/spool/cron/crontabs/root (no username field)
        try:
            with open("/var/spool/cron/crontabs/root", "r") as f:
                for line in f:
                    job = parse_line(line, "crontabs/root", has_username=False)
                    if job:
                        jobs.append(job)
        except FileNotFoundError:
            pass

        # 2. System crontab: /etc/crontab (has username field)
        try:
            with open("/etc/crontab", "r") as f:
                for line in f:
                    job = parse_line(line, "/etc/crontab", has_username=True)
                    if job:
                        jobs.append(job)
        except FileNotFoundError:
            pass

        # 3. /etc/cron.d/* (has username field)
        cron_d = "/etc/cron.d"
        if os.path.isdir(cron_d):
            for fname in sorted(os.listdir(cron_d)):
                if fname.startswith("."):
                    continue
                fpath = os.path.join(cron_d, fname)
                if not os.path.isfile(fpath):
                    continue
                try:
                    with open(fpath, "r") as f:
                        for line in f:
                            job = parse_line(line, f"/etc/cron.d/{fname}", has_username=True)
                            if job:
                                jobs.append(job)
                except Exception:
                    pass

        # 4. Hermes cron jobs from ~/.hermes/cron/jobs.json
        hermes_cron_path = os.path.join(HERMES_HOME, "cron", "jobs.json")
        if os.path.isfile(hermes_cron_path):
            try:
                with open(hermes_cron_path, "r") as f:
                    hc = json.load(f)
                for j in (hc.get("jobs") or []):
                    if not j.get("enabled", True):
                        continue
                    sched = j.get("schedule", {})
                    expr = sched.get("expr", "") if isinstance(sched, dict) else str(sched)
                    parts = expr.strip().split() if expr else []
                    english = _cron_to_english(parts) if len(parts) >= 5 else expr

                    # Convert UTC cron time to UTC+3 for display
                    if len(parts) >= 2:
                        english = _cron_to_english_tz(parts, TZ_SAMI)

                    # Extract a friendly name from the prompt
                    name = j.get("name") or "unnamed"
                    prompt = j.get("prompt", "")
                    # Short description: first line of prompt, stripped
                    desc = prompt.split("\n")[0][:80] if prompt else english

                    # Next run time
                    next_run = j.get("next_run_at")
                    next_display = None
                    if next_run:
                        try:
                            dt = datetime.fromisoformat(next_run.replace("Z", "+00:00"))
                            next_display = dt.astimezone(TZ_SAMI).strftime("%Y-%m-%d %H:%M")
                        except Exception:
                            next_display = next_run

                    jobs.append({
                        "schedule": expr,
                        "english": english,
                        "detail": english,
                        "name": name,
                        "user": "hermes",
                        "command": desc,
                        "source": "hermes-cron",
                        "label": "hermes",
                        "category": "hermes",
                        "next_run": next_display,
                        "skills": j.get("skills", []),
                        "last_run": j.get("last_run_at"),
                        "last_status": j.get("last_status"),
                    })
            except Exception:
                pass

        return {"jobs": jobs, "count": len(jobs)}
    except Exception as e:
        return {"error": f"cron_jobs: {e}", "jobs": [], "count": 0}


# ─── Personal Operator Task Board ─────────────────────────────────────────────

_board_lock = threading.Lock()


def board_init():
    """Initialize board.db with schema and seed data on first run."""
    conn = sqlite3.connect(BOARD_DB)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            priority TEXT DEFAULT 'medium',
            notes TEXT DEFAULT '',
            assignee TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT
        )
    """)
    conn.commit()

    # Check if already seeded
    cur.execute("SELECT COUNT(*) FROM tasks")
    if cur.fetchone()[0] == 0:
        now = datetime.now(TZ_SAMI).isoformat()
        seeds = [
            ("Review agent-logs schema for dashboard integration", "completed", "high", "Confirmed columns and swapped model_used/status mapping"),
            ("Set up Discord channels for all 4 specialist agents", "in_progress", "high", "Channels #analyst #writer #marketer #coder created; bot permissions pending"),
            ("Configure API keys per agent profile", "pending", "high", "Need separate API keys for analyst, writer, marketer, coder profiles"),
            ("Test full content pipeline end-to-end", "pending", "medium", "Analyst→Writer→Marketer chain with real topic"),
            ("Document multi-agent architecture in shared workspace", "in_progress", "medium", "Draft in /root/agents/_shared/architecture.md"),
            ("Set up monthly log retention for all agent profiles", "completed", "low", "Cron job at 00:00 on 1st of month, cleanup-logs.sh deployed"),
            ("Wire Telegram alerts for agent failures", "pending", "medium", "Bot already connected; need failure-detection hook in Orchestrator"),
            ("Create monitoring dashboard for system health", "in_progress", "high", "Mission control dashboard — this project"),
        ]
        for title, status, priority, notes in seeds:
            cur.execute(
                "INSERT INTO tasks (id, title, status, priority, notes, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (str(uuid.uuid4()), title, status, priority, notes, now, now)
            )
        conn.commit()

    conn.close()


def board_list():
    """List all tasks from the board."""
    try:
        with _board_lock:
            conn = sqlite3.connect(BOARD_DB)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM tasks ORDER BY created_at DESC")
            tasks = [dict(r) for r in cur.fetchall()]
            conn.close()
        return {"tasks": tasks, "count": len(tasks)}
    except Exception as e:
        return {"error": f"board_list: {e}", "tasks": [], "count": 0}


def board_create(body):
    """Create a new task."""
    try:
        data = json.loads(body) if body else {}
        now = datetime.now(TZ_SAMI).isoformat()
        task_id = str(uuid.uuid4())
        title = data.get("title", "Untitled")
        status = data.get("status", "pending")
        priority = data.get("priority", "medium")
        notes = data.get("notes", "")
        assignee = data.get("assignee", "")

        with _board_lock:
            conn = sqlite3.connect(BOARD_DB)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO tasks (id, title, status, priority, notes, assignee, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (task_id, title, status, priority, notes, assignee, now, now)
            )
            conn.commit()
            conn.close()

        return {"ok": True, "id": task_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def board_update(task_id, body):
    """Update fields on a task."""
    try:
        data = json.loads(body) if body else {}
        now = datetime.now(TZ_SAMI).isoformat()

        allowed = {"title", "status", "priority", "notes", "assignee"}
        updates = {k: v for k, v in data.items() if k in allowed}
        if not updates:
            return {"ok": False, "error": "No valid fields to update"}

        set_clauses = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [now, task_id]

        with _board_lock:
            conn = sqlite3.connect(BOARD_DB)
            cur = conn.cursor()
            cur.execute(
                f"UPDATE tasks SET {set_clauses}, updated_at = ? WHERE id = ?",
                values
            )
            conn.commit()
            conn.close()

        return {"ok": True, "id": task_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def board_delete(task_id):
    """Delete a task by id."""
    try:
        with _board_lock:
            conn = sqlite3.connect(BOARD_DB)
            cur = conn.cursor()
            cur.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            conn.commit()
            conn.close()
        return {"ok": True, "id": task_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ─── Content Library ──────────────────────────────────────────────────────────

CONTENT_ROOT = os.path.join(HERMES_HOME, "content")
CONTENT_AGENTS = {"orchestrator", "analyst", "writer", "marketer", "coder"}


def content_list():
    """Scan all agent subfolders and return a list of document metadata."""
    docs = []
    if not os.path.isdir(CONTENT_ROOT):
        return []
    for agent in sorted(CONTENT_AGENTS):
        agent_dir = os.path.join(CONTENT_ROOT, agent)
        if not os.path.isdir(agent_dir):
            continue
        for fname in os.listdir(agent_dir):
            if not fname.endswith(".md"):
                continue
            fpath = os.path.join(agent_dir, fname)
            title = fname
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    first_line = f.readline().strip()
                    if first_line.startswith("#"):
                        title = first_line.lstrip("#").strip()
            except Exception:
                pass
            mtime = os.path.getmtime(fpath)
            mod_iso = datetime.fromtimestamp(mtime, tz=TZ_SAMI).isoformat()
            docs.append({
                "agent": agent,
                "filename": fname,
                "title": title,
                "modified_at": mod_iso,
            })
    docs.sort(key=lambda d: d["modified_at"], reverse=True)
    return docs


def _content_validate(agent, filename):
    """Validate agent and filename. Returns (ok, full_path_or_error)."""
    if agent not in CONTENT_AGENTS:
        return (False, "Invalid agent")
    if ".." in filename or "/" in filename or "\\" in filename:
        return (False, "Invalid filename")
    if not filename.endswith(".md"):
        return (False, "Only .md files allowed")
    agent_dir = os.path.join(CONTENT_ROOT, agent)
    return (True, os.path.join(agent_dir, filename))


def content_read(agent, filename):
    """Read a markdown file and return its content."""
    ok, result = _content_validate(agent, filename)
    if not ok:
        return {"error": result}
    if not os.path.isfile(result):
        return {"error": "File not found"}
    try:
        with open(result, "r", encoding="utf-8") as f:
            text = f.read()
        return {"text": text}
    except Exception as e:
        return {"error": str(e)}


def content_save(agent, filename, body_bytes):
    """Create or update a markdown file."""
    ok, result = _content_validate(agent, filename)
    if not ok:
        return {"ok": False, "error": result}
    agent_dir = os.path.join(CONTENT_ROOT, agent)
    os.makedirs(agent_dir, exist_ok=True)
    try:
        with open(result, "wb") as f:
            f.write(body_bytes)
        return {"ok": True, "path": result}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def content_delete(agent, filename):
    """Delete a markdown file."""
    ok, result = _content_validate(agent, filename)
    if not ok:
        return {"ok": False, "error": result}
    if not os.path.isfile(result):
        return {"ok": False, "error": "File not found", "status": 404}
    try:
        os.unlink(result)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ─── Full Snapshot ────────────────────────────────────────────────────────────

def build_snapshot():
    """Call all five data functions and return a combined snapshot dict."""
    activity = activity_data()
    sessions = sessions_data()
    vps = vps_health()
    gateway = gateway_data()

    # Build top-level stats from activity + sessions
    act_stats = activity.get("stats", {}) if not activity.get("error") else {}
    sess_totals = sessions.get("totals", {}) if not sessions.get("error") else {}

    return {
        "timestamp": datetime.now(TZ_SAMI).isoformat(),
        "gateway": gateway,
        "activity": activity,
        "sessions": sessions,
        "vps": vps,
        "cron": cron_jobs(),
        "crons": cron_jobs().get("jobs", []),
        "agents": activity.get("agents", []),
        "stats": {
            "total": act_stats.get("total", 0),
            "completed": act_stats.get("completed", 0),
            "failed": act_stats.get("failed", 0),
            "messages": sess_totals.get("messages", 0),
            "input_tokens": sess_totals.get("input_tokens", 0),
            "cache_read_tokens": sess_totals.get("cache_read_tokens", 0),
        },
        "kanban": {
            "total": 0,  # kanban.db is empty, no tasks yet
        },
    }


# ─── HTTP Handler ─────────────────────────────────────────────────────────────

INDEX_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        # Suppress default request logging to keep stdout clean
        pass

    def _send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, path):
        try:
            with open(path, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self._send_json({"error": "index.html not found"}, 404)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == "/" or path == "/index.html":
            self._send_html(INDEX_HTML)

        elif path == "/api/snapshot":
            self._send_json(build_snapshot())

        elif path == "/api/board":
            self._send_json(board_list())

        elif path == "/api/content":
            self._send_json(content_list())

        elif path == "/api/content/read":
            agent = qs.get("agent", [None])[0]
            fname = qs.get("file", [None])[0]
            if not agent or not fname:
                self._send_json({"error": "Missing agent or file parameter"}, 400)
                return
            self._send_json(content_read(agent, fname))

        elif path == "/events":
            self._handle_sse()

        else:
            self._send_json({"error": f"Not found: {path}"}, 404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        # Read body
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else ""

        if path == "/api/board":
            self._send_json(board_create(body))

        elif path == "/api/board/update":
            task_id = qs.get("id", [None])[0]
            if not task_id:
                self._send_json({"ok": False, "error": "Missing id parameter"}, 400)
                return
            self._send_json(board_update(task_id, body))

        elif path == "/api/board/delete":
            task_id = qs.get("id", [None])[0]
            if not task_id:
                self._send_json({"ok": False, "error": "Missing id parameter"}, 400)
                return
            self._send_json(board_delete(task_id))

        elif path == "/api/content/save":
            agent = qs.get("agent", [None])[0]
            fname = qs.get("file", [None])[0]
            if not agent or not fname:
                self._send_json({"ok": False, "error": "Missing agent or file parameter"}, 400)
                return
            self._send_json(content_save(agent, fname, body.encode("utf-8")))

        elif path == "/api/content/delete":
            agent = qs.get("agent", [None])[0]
            fname = qs.get("file", [None])[0]
            if not agent or not fname:
                self._send_json({"ok": False, "error": "Missing agent or file parameter"}, 400)
                return
            result = content_delete(agent, fname)
            status = 404 if result.get("status") == 404 else 200
            self._send_json(result, status)

        else:
            self._send_json({"error": f"Not found: {path}"}, 404)

    def _handle_sse(self):
        """Server-Sent Events: push /api/snapshot every SSE_INTERVAL seconds."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        try:
            while True:
                snapshot = build_snapshot()
                payload = json.dumps(snapshot, default=str)
                self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                self.wfile.flush()
                time.sleep(SSE_INTERVAL)
        except (BrokenPipeError, ConnectionResetError):
            # Client disconnected
            pass
        except Exception:
            pass


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    # Initialize the personal board DB
    board_init()

    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Mission Control running on http://{HOST}:{PORT}")
    print(f"  GET  /              → dashboard")
    print(f"  GET  /api/snapshot  → JSON snapshot")
    print(f"  GET  /events        → SSE live updates (every {SSE_INTERVAL}s)")
    print(f"  GET  /api/board     → list tasks")
    print(f"  POST /api/board     → create task")
    print(f"  POST /api/board/update?id=  → update task")
    print(f"  POST /api/board/delete?id=  → delete task")
    print(f"  board.db: {BOARD_DB}")
    print()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
