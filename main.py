#!/usr/bin/env python3
"""
ADMS Receiver Server
Server penerima data ZKTeco ADMS dari mesin BioFinger AT301.

Fitur:
1. HTTP server protokol ZKTeco ADMS (port 8000)
2. Scheduler kirim batch rekap ke HRD Dashboard 4x sehari
3. Command queue per-device untuk inject user & fingerprint
4. Sinkronisasi fingerprint otomatis antar semua mesin
"""

import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Query, Request
from fastapi.responses import PlainTextResponse

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
HRD_API_URL = os.environ.get(
    "HRD_API_URL",
    "https://hris.it-teknos.site/api/integrations/adms/attendance",
)
ADMS_INGEST_TOKEN = os.environ.get("ADMS_INGEST_TOKEN", "")
DB_PATH = os.environ.get("DB_PATH", "/var/lib/adms-receiver/punches.db")

_parsed = urlparse(HRD_API_URL)
HRD_BASE_URL = f"{_parsed.scheme}://{_parsed.netloc}"
HRD_EMPLOYEES_URL = f"{HRD_BASE_URL}/api/integrations/adms/employees"

WIB = timezone(timedelta(hours=7))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("adms")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS punch_logs (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                device_sn     TEXT    NOT NULL,
                employee_code TEXT    NOT NULL,
                punch_time    TEXT    NOT NULL,
                punch_type    INTEGER NOT NULL,
                synced        INTEGER NOT NULL DEFAULT 0,
                created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_punch_logs_date
            ON punch_logs (employee_code, punch_time, synced)
        """)
        # Daftar perangkat yang pernah terhubung
        conn.execute("""
            CREATE TABLE IF NOT EXISTS devices (
                sn         TEXT PRIMARY KEY,
                last_seen  TEXT NOT NULL
            )
        """)
        # Penyimpanan template fingerprint dari semua mesin
        conn.execute("""
            CREATE TABLE IF NOT EXISTS biodata_store (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_code TEXT    NOT NULL,
                finger_id     INTEGER NOT NULL,
                template_size INTEGER,
                valid         INTEGER NOT NULL DEFAULT 1,
                template_data TEXT    NOT NULL,
                source_device TEXT    NOT NULL,
                updated_at    TEXT    NOT NULL DEFAULT (datetime('now')),
                UNIQUE(employee_code, finger_id)
            )
        """)
        # Command queue per-device
        conn.execute("""
            CREATE TABLE IF NOT EXISTS command_queue (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                target_device TEXT    NOT NULL,
                command_text  TEXT    NOT NULL,
                sent_at       TEXT,
                acked_at      TEXT,
                created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cmd_queue_pending
            ON command_queue (target_device, sent_at)
        """)


# ---------------------------------------------------------------------------
# Device registry helpers
# ---------------------------------------------------------------------------
def register_device(sn: str):
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO devices (sn, last_seen) VALUES (?, ?)",
            (sn, datetime.now(WIB).isoformat()),
        )


def get_all_device_sns() -> list[str]:
    with get_db() as conn:
        rows = conn.execute("SELECT sn FROM devices").fetchall()
    return [r["sn"] for r in rows]


# ---------------------------------------------------------------------------
# Fingerprint sync
# ---------------------------------------------------------------------------
def store_biodata(device_sn: str, raw_body: str):
    """Simpan template fingerprint dari mesin dan queue ke semua mesin lain."""
    new_templates: list[tuple] = []

    with get_db() as conn:
        for line in raw_body.strip().splitlines():
            parts = line.strip().split("\t")
            if len(parts) < 5:
                continue
            employee_code = parts[0].strip()
            try:
                finger_id = int(parts[1].strip())
                template_size = int(parts[2].strip())
                valid = int(parts[3].strip())
            except (ValueError, IndexError):
                continue
            template_data = parts[4].strip()
            if not template_data or valid != 1:
                continue

            conn.execute("""
                INSERT INTO biodata_store
                    (employee_code, finger_id, template_size, valid, template_data, source_device, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(employee_code, finger_id) DO UPDATE SET
                    template_size = excluded.template_size,
                    valid         = excluded.valid,
                    template_data = excluded.template_data,
                    source_device = excluded.source_device,
                    updated_at    = excluded.updated_at
            """, (employee_code, finger_id, template_size, valid,
                  template_data, device_sn, datetime.now(WIB).isoformat()))

            new_templates.append((employee_code, finger_id, template_size, template_data))

    if new_templates:
        log.info(f"[{device_sn}] Tersimpan {len(new_templates)} fingerprint template.")
        _queue_fingerprints_to_other_devices(device_sn, new_templates)


def _queue_fingerprints_to_other_devices(source_sn: str, templates: list[tuple]):
    """Queue DATA FP command ke semua mesin kecuali sumber."""
    other_sns = [sn for sn in get_all_device_sns() if sn != source_sn]
    if not other_sns:
        return

    with get_db() as conn:
        for target_sn in other_sns:
            for emp_code, finger_id, template_size, template_data in templates:
                cmd = (
                    f"DATA FP PIN={emp_code}\tFID={finger_id}"
                    f"\tSize={template_size}\tValid=1\tTEMPLATE={template_data}"
                )
                conn.execute(
                    "INSERT INTO command_queue (target_device, command_text) VALUES (?, ?)",
                    (target_sn, cmd),
                )
    log.info(f"Queued {len(templates)} fingerprint(s) ke {len(other_sns)} mesin lain.")


# ---------------------------------------------------------------------------
# Punch log
# ---------------------------------------------------------------------------
def store_punches(device_sn: str, raw_body: str):
    rows_saved = 0
    with get_db() as conn:
        for line in raw_body.strip().splitlines():
            parts = line.strip().split("\t")
            if len(parts) < 3:
                continue
            employee_code = parts[0].strip()
            punch_time_str = parts[1].strip()
            try:
                punch_type = int(parts[2].strip())
                datetime.strptime(punch_time_str, "%Y-%m-%d %H:%M:%S")
            except (ValueError, IndexError):
                continue
            conn.execute(
                "INSERT OR IGNORE INTO punch_logs (device_sn, employee_code, punch_time, punch_type) VALUES (?, ?, ?, ?)",
                (device_sn, employee_code, punch_time_str, punch_type),
            )
            rows_saved += 1
    if rows_saved:
        log.info(f"[{device_sn}] Tersimpan {rows_saved} punch baru.")


# ---------------------------------------------------------------------------
# Batch sender
# ---------------------------------------------------------------------------
PUNCH_MASUK = 0
PUNCH_PULANG = 1
PUNCH_KELUAR_ISTIRAHAT = 2
PUNCH_MASUK_ISTIRAHAT = 3


def send_batch_for_date(target_date: date):
    if not ADMS_INGEST_TOKEN:
        log.warning("ADMS_INGEST_TOKEN belum diset, batch dilewati.")
        return

    date_str = target_date.isoformat()
    date_prefix = f"{date_str} "

    with get_db() as conn:
        rows = conn.execute("""
            SELECT device_sn, employee_code, punch_time, punch_type
            FROM punch_logs
            WHERE punch_time >= ? AND punch_time < ?
            ORDER BY punch_time ASC
        """, (date_prefix + "00:00:00", date_prefix + "23:59:59")).fetchall()

    if not rows:
        log.info(f"[{date_str}] Tidak ada data punch, batch dilewati.")
        return

    groups: dict[tuple, dict] = {}
    for row in rows:
        key = (row["device_sn"], row["employee_code"])
        if key not in groups:
            groups[key] = {
                "device_sn": row["device_sn"],
                "employee_code": row["employee_code"],
                "check_in": None, "check_out": None,
                "break_out": None, "break_in": None,
            }
        g = groups[key]
        t = row["punch_time"][11:16]
        if row["punch_type"] == PUNCH_MASUK and not g["check_in"]:
            g["check_in"] = t
        elif row["punch_type"] == PUNCH_PULANG:
            g["check_out"] = t
        elif row["punch_type"] == PUNCH_KELUAR_ISTIRAHAT and not g["break_out"]:
            g["break_out"] = t
        elif row["punch_type"] == PUNCH_MASUK_ISTIRAHAT and not g["break_in"]:
            g["break_in"] = t

    by_device: dict[str, list] = {}
    for (device_sn, emp_code), g in groups.items():
        record: dict = {"employeeCode": emp_code, "attendanceDate": date_str, "attendanceStatus": "HADIR"}
        if g["check_in"]: record["checkInTime"] = g["check_in"]
        if g["check_out"]: record["checkOutTime"] = g["check_out"]
        if g["break_out"]: record["breakOutTime"] = g["break_out"]
        if g["break_in"]: record["breakInTime"] = g["break_in"]
        by_device.setdefault(device_sn, []).append(record)

    for device_sn, records in by_device.items():
        try:
            resp = requests.post(
                HRD_API_URL,
                headers={"Authorization": f"Bearer {ADMS_INGEST_TOKEN}", "Content-Type": "application/json"},
                json={"deviceId": device_sn, "records": records},
                timeout=30,
            )
            resp.raise_for_status()
            result = resp.json()
            log.info(
                f"[{device_sn}] {date_str} — "
                f"inserted:{result.get('inserted',0)} "
                f"updated:{result.get('updated',0)} "
                f"skipped:{result.get('skipped',0)}"
            )
            if result.get("errors"):
                for err in result["errors"][:5]:
                    log.warning(f"  skip [{err.get('employeeCode')}]: {err.get('reason')}")
        except Exception as exc:
            log.error(f"[{device_sn}] Gagal kirim batch: {exc}")


def scheduled_sync():
    today = datetime.now(WIB).date()
    yesterday = today - timedelta(days=1)
    log.info(f"=== Scheduled sync: {today} ===")
    send_batch_for_date(today)
    send_batch_for_date(yesterday)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app_router = FastAPI(title="ADMS Receiver")


@app_router.get("/iclock/cdata", response_class=PlainTextResponse)
async def device_handshake(
    SN: str = Query(...),
    options: str = Query(None),
    pushver: str = Query(None),
):
    register_device(SN)
    log.info(f"[{SN}] Handshake dari mesin.")
    config = (
        f"GET OPTION FROM: {SN}\r\n"
        f"ATTLOGStamp=0\r\n"
        f"OPERLOGStamp=9999\r\n"
        f"BIODATAStamp=0\r\n"
        f"ErrorDelay=30\r\n"
        f"Delay=10\r\n"
        f"TransTimes=09:00;14:00;17:00;21:00\r\n"
        f"TransInterval=1\r\n"
        f"TransFlag=TransData AttLog OpLog BiometricData\r\n"
        f"TimeZone=7\r\n"
        f"Realtime=1\r\n"
        f"Encrypt=None\r\n"
    )
    return PlainTextResponse(config)


@app_router.post("/iclock/cdata", response_class=PlainTextResponse)
async def receive_data(
    request: Request,
    SN: str = Query(...),
    table: str = Query(None),
    Stamp: str = Query(None),
):
    body = await request.body()
    raw = body.decode("utf-8", errors="ignore")

    if table == "ATTLOG":
        store_punches(SN, raw)
    elif table == "BIODATA":
        store_biodata(SN, raw)
    else:
        log.debug(f"[{SN}] Tabel {table} diabaikan.")

    return PlainTextResponse("OK")


@app_router.get("/iclock/getrequest", response_class=PlainTextResponse)
async def get_request(SN: str = Query(...), INFO: str = Query(None)):
    register_device(SN)
    with get_db() as conn:
        row = conn.execute("""
            SELECT id, command_text FROM command_queue
            WHERE target_device = ? AND sent_at IS NULL
            LIMIT 1
        """, (SN,)).fetchone()
        if row:
            conn.execute(
                "UPDATE command_queue SET sent_at = ? WHERE id = ?",
                (datetime.now(WIB).isoformat(), row["id"]),
            )
            log.info(f"[{SN}] Mengirim command #{row['id']}: {row['command_text'][:60]}...")
            return PlainTextResponse(f"C:{row['id']}:{row['command_text']}")
    return PlainTextResponse("OK")


@app_router.post("/iclock/devicecmd", response_class=PlainTextResponse)
async def device_cmd(
    SN: str = Query(...),
    ID: str = Query(None),
    Return: str = Query(None),
):
    if ID:
        try:
            cmd_id = int(ID)
            with get_db() as conn:
                conn.execute(
                    "UPDATE command_queue SET acked_at = ? WHERE id = ?",
                    (datetime.now(WIB).isoformat(), cmd_id),
                )
            log.info(f"[{SN}] Command #{cmd_id} dikonfirmasi (Return={Return})")
        except Exception as exc:
            log.warning(f"[{SN}] Gagal konfirmasi command {ID}: {exc}")
    return PlainTextResponse("OK")


# ---------------------------------------------------------------------------
# Management endpoints
# ---------------------------------------------------------------------------
@app_router.get("/health")
async def health():
    devices = get_all_device_sns()
    with get_db() as conn:
        pending = conn.execute(
            "SELECT COUNT(*) as n FROM command_queue WHERE sent_at IS NULL"
        ).fetchone()["n"]
        fp_count = conn.execute(
            "SELECT COUNT(*) as n FROM biodata_store"
        ).fetchone()["n"]
    return {
        "status": "ok",
        "time": datetime.now(WIB).isoformat(),
        "devices": devices,
        "pending_commands": pending,
        "fingerprints_stored": fp_count,
    }


@app_router.get("/inject-employees")
async def inject_employees():
    """Fetch karyawan aktif dari HRD dan queue DATA USER ke semua mesin terdaftar."""
    if not ADMS_INGEST_TOKEN:
        return {"error": "ADMS_INGEST_TOKEN belum diset."}

    try:
        resp = requests.get(
            HRD_EMPLOYEES_URL,
            headers={"Authorization": f"Bearer {ADMS_INGEST_TOKEN}"},
            timeout=30,
        )
        resp.raise_for_status()
        emp_list = resp.json().get("employees", [])
    except Exception as exc:
        return {"error": f"Gagal fetch karyawan dari HRD: {exc}"}

    if not emp_list:
        return {"status": "ok", "queued": 0, "message": "Tidak ada karyawan aktif."}

    device_sns = get_all_device_sns()
    if not device_sns:
        return {"error": "Belum ada mesin terdaftar. Pastikan mesin sudah handshake ke server."}

    with get_db() as conn:
        # Reset antrian user (jangan hapus fingerprint queue)
        conn.execute("DELETE FROM command_queue WHERE sent_at IS NULL AND command_text LIKE 'DATA USER%'")
        for sn in device_sns:
            for emp in emp_list:
                pin = emp["employeeCode"]
                name = emp["fullName"][:24]
                cmd = f"DATA USER PIN={pin}\tName={name}\tPri=0\tPasswd=\tCard=\tGrp=1\tTZ=0000000100000000\tVerify=0"
                conn.execute(
                    "INSERT INTO command_queue (target_device, command_text) VALUES (?, ?)",
                    (sn, cmd),
                )

    total = len(emp_list) * len(device_sns)
    log.info(f"Antrian inject: {len(emp_list)} karyawan × {len(device_sns)} mesin = {total} command.")
    return {"status": "ok", "employees": len(emp_list), "devices": device_sns, "total_queued": total}


@app_router.get("/inject-fingerprints")
async def inject_fingerprints(target: str = Query(None, description="SN mesin tujuan, kosong = semua mesin")):
    """Push semua fingerprint tersimpan ke mesin tertentu atau semua mesin."""
    with get_db() as conn:
        templates = conn.execute(
            "SELECT employee_code, finger_id, template_size, template_data FROM biodata_store WHERE valid = 1"
        ).fetchall()

    if not templates:
        return {"status": "ok", "queued": 0, "message": "Belum ada fingerprint tersimpan."}

    device_sns = [target] if target else get_all_device_sns()
    if not device_sns:
        return {"error": "Belum ada mesin terdaftar."}

    with get_db() as conn:
        if target:
            conn.execute(
                "DELETE FROM command_queue WHERE target_device = ? AND sent_at IS NULL AND command_text LIKE 'DATA FP%'",
                (target,),
            )
        else:
            conn.execute("DELETE FROM command_queue WHERE sent_at IS NULL AND command_text LIKE 'DATA FP%'")

        for sn in device_sns:
            for t in templates:
                cmd = (
                    f"DATA FP PIN={t['employee_code']}\tFID={t['finger_id']}"
                    f"\tSize={t['template_size']}\tValid=1\tTEMPLATE={t['template_data']}"
                )
                conn.execute(
                    "INSERT INTO command_queue (target_device, command_text) VALUES (?, ?)",
                    (sn, cmd),
                )

    total = len(templates) * len(device_sns)
    log.info(f"Inject fingerprint: {len(templates)} template × {len(device_sns)} mesin = {total} command.")
    return {"status": "ok", "fingerprints": len(templates), "devices": device_sns, "total_queued": total}


@app_router.get("/devices")
async def list_devices():
    """Daftar semua mesin yang pernah terhubung."""
    with get_db() as conn:
        rows = conn.execute("SELECT sn, last_seen FROM devices ORDER BY last_seen DESC").fetchall()
        pending = conn.execute("""
            SELECT target_device, COUNT(*) as n
            FROM command_queue WHERE sent_at IS NULL
            GROUP BY target_device
        """).fetchall()
    pending_map = {r["target_device"]: r["n"] for r in pending}
    return {
        "devices": [
            {"sn": r["sn"], "last_seen": r["last_seen"], "pending_commands": pending_map.get(r["sn"], 0)}
            for r in rows
        ]
    }


@app_router.get("/sync/{target_date}")
async def trigger_sync(target_date: str):
    try:
        d = date.fromisoformat(target_date)
    except ValueError:
        return {"error": "Format tanggal tidak valid. Gunakan YYYY-MM-DD."}
    send_batch_for_date(d)
    return {"status": "ok", "synced_date": target_date}


@app_router.get("/sync-now")
async def trigger_sync_now():
    scheduled_sync()
    return {"status": "ok", "time": datetime.now(WIB).isoformat()}


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    log.info(f"DB siap: {DB_PATH}")
    scheduler = BackgroundScheduler(timezone="Asia/Jakarta")
    for hour in [9, 14, 17, 21]:
        scheduler.add_job(scheduled_sync, "cron", hour=hour, minute=5)
    scheduler.start()
    log.info("Scheduler aktif: sync jam 09:05, 14:05, 17:05, 21:05 WIB")
    yield
    scheduler.shutdown()


app_router.router.lifespan_context = lifespan

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app_router", host="0.0.0.0", port=8000, reload=False, log_level="info")
