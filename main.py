#!/usr/bin/env python3
"""
ADMS Receiver — ZKTeco AT301
Menerima absensi & fingerprint dari mesin, kirim ke HRD Dashboard.

Env vars (Coolify):
  DATABASE_URL       — postgres connection string Supabase (PgBouncer port 5433)
  ADMS_INGEST_TOKEN  — bearer token untuk HRD Dashboard API
  HRD_API_URL        — (opsional) override URL attendance endpoint
"""

import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Query, Request
from fastapi.responses import PlainTextResponse

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "")
HRD_API_URL = os.environ.get(
    "HRD_API_URL",
    "https://hris.it-teknos.site/api/integrations/adms/attendance",
)
ADMS_INGEST_TOKEN = os.environ.get("ADMS_INGEST_TOKEN", "")

_parsed = urlparse(HRD_API_URL)
HRD_EMPLOYEES_URL = f"{_parsed.scheme}://{_parsed.netloc}/api/integrations/adms/employees"

WIB = timezone(timedelta(hours=7))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("adms")


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def db_one(sql: str, params: tuple = ()):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


def db_all(sql: str, params: tuple = ()):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def db_run(sql: str, params: tuple = ()):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()


def db_run_many(statements: list[tuple]):
    with get_db() as conn:
        with conn.cursor() as cur:
            for sql, params in statements:
                cur.execute(sql, params)
        conn.commit()


# ---------------------------------------------------------------------------
# Device registry
# ---------------------------------------------------------------------------
def register_device(sn: str):
    db_run("""
        INSERT INTO adms_devices (sn, last_seen)
        VALUES (%s, NOW())
        ON CONFLICT (sn) DO UPDATE SET last_seen = NOW()
    """, (sn,))


def get_all_device_sns() -> list[str]:
    return [r["sn"] for r in db_all("SELECT sn FROM adms_devices")]


# ---------------------------------------------------------------------------
# Fingerprint storage — mendukung dua format firmware:
#   key=value : Pin=0067\tFid=0\tSize=498\tValid=1\ttmp=BASE64  (fw8.x FPTrans)
#   positional: code\tfinger_id\tsize\tvalid\ttemplate           (format lama)
# ---------------------------------------------------------------------------
def store_biodata(device_sn: str, raw_body: str) -> int:
    statements = []

    for line in raw_body.strip().splitlines():
        line = line.strip()
        if not line:
            continue

        if "\t" in line and "=" in line.split("\t")[0]:
            # Format key=value
            fields: dict[str, str] = {}
            for part in line.split("\t"):
                if "=" in part:
                    k, _, v = part.partition("=")
                    fields[k.strip().lower()] = v.strip()
            employee_code = fields.get("pin")
            finger_id_str = fields.get("fid") or fields.get("type")
            template_data = fields.get("tmp") or fields.get("template")
            if not employee_code or finger_id_str is None or not template_data:
                continue
            try:
                finger_id = int(finger_id_str)
                valid = int(fields.get("valid", "1"))
                template_size = int(fields.get("size", "0")) or len(template_data)
            except ValueError:
                continue
        else:
            # Format positional
            parts = line.split("\t")
            if len(parts) < 5:
                continue
            try:
                employee_code = parts[0].strip()
                finger_id = int(parts[1].strip())
                template_size = int(parts[2].strip())
                valid = int(parts[3].strip())
                template_data = parts[4].strip()
            except (ValueError, IndexError):
                continue

        if not template_data or valid != 1:
            continue

        statements.append(("""
            INSERT INTO adms_biodata
                (employee_code, finger_id, template_size, valid, template_data, source_device, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (employee_code, finger_id) DO UPDATE SET
                template_size = EXCLUDED.template_size,
                valid         = EXCLUDED.valid,
                template_data = EXCLUDED.template_data,
                source_device = EXCLUDED.source_device,
                updated_at    = NOW()
        """, (employee_code, finger_id, template_size, valid, template_data, device_sn)))

    if statements:
        db_run_many(statements)
        log.info(f"[{device_sn}] {len(statements)} fingerprint tersimpan.")

    return len(statements)


# ---------------------------------------------------------------------------
# Attendance storage
# ---------------------------------------------------------------------------
def store_punches(device_sn: str, raw_body: str):
    statements = []
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
        statements.append(("""
            INSERT INTO adms_punch_logs (device_sn, employee_code, punch_time, punch_type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (device_sn, employee_code, punch_time) DO NOTHING
        """, (device_sn, employee_code, punch_time_str, punch_type)))

    if statements:
        db_run_many(statements)
        log.info(f"[{device_sn}] {len(statements)} punch tersimpan.")


# ---------------------------------------------------------------------------
# Batch sender ke HRD Dashboard
# ---------------------------------------------------------------------------
PUNCH_MASUK           = 0
PUNCH_PULANG          = 1
PUNCH_KELUAR_ISTIRAHAT = 2
PUNCH_MASUK_ISTIRAHAT  = 3


def send_batch_for_date(target_date: date):
    if not ADMS_INGEST_TOKEN:
        log.warning("ADMS_INGEST_TOKEN belum diset, batch dilewati.")
        return

    date_str = target_date.isoformat()
    rows = db_all("""
        SELECT device_sn, employee_code, punch_time, punch_type
        FROM adms_punch_logs
        WHERE punch_time::date = %s
        ORDER BY punch_time ASC
    """, (date_str,))

    if not rows:
        log.info(f"[{date_str}] Tidak ada data punch.")
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
        t = row["punch_time"].strftime("%H:%M")
        pt = row["punch_type"]
        if pt == PUNCH_MASUK and not g["check_in"]:
            g["check_in"] = t
        elif pt == PUNCH_PULANG:
            g["check_out"] = t
        elif pt == PUNCH_KELUAR_ISTIRAHAT and not g["break_out"]:
            g["break_out"] = t
        elif pt == PUNCH_MASUK_ISTIRAHAT and not g["break_in"]:
            g["break_in"] = t

    by_device: dict[str, list] = {}
    for (device_sn, emp_code), g in groups.items():
        record: dict = {
            "employeeCode": emp_code,
            "attendanceDate": date_str,
            "attendanceStatus": "HADIR",
        }
        if g["check_in"]:  record["checkInTime"]  = g["check_in"]
        if g["check_out"]: record["checkOutTime"] = g["check_out"]
        if g["break_out"]: record["breakOutTime"] = g["break_out"]
        if g["break_in"]:  record["breakInTime"]  = g["break_in"]
        by_device.setdefault(device_sn, []).append(record)

    for device_sn, records in by_device.items():
        try:
            resp = requests.post(
                HRD_API_URL,
                headers={
                    "Authorization": f"Bearer {ADMS_INGEST_TOKEN}",
                    "Content-Type": "application/json",
                },
                json={"deviceId": device_sn, "records": records},
                timeout=30,
            )
            resp.raise_for_status()
            result = resp.json()
            log.info(
                f"[{device_sn}] {date_str} — "
                f"inserted:{result.get('inserted', 0)} "
                f"updated:{result.get('updated', 0)} "
                f"skipped:{result.get('skipped', 0)}"
            )
            for err in (result.get("errors") or [])[:5]:
                log.warning(f"  skip [{err.get('employeeCode')}]: {err.get('reason')}")
        except Exception as exc:
            log.error(f"[{device_sn}] Gagal kirim batch {date_str}: {exc}")


def scheduled_sync():
    today = datetime.now(WIB).date()
    log.info(f"=== Scheduled sync: {today} ===")
    send_batch_for_date(today)
    send_batch_for_date(today - timedelta(days=1))


# ---------------------------------------------------------------------------
# FastAPI — ADMS protocol endpoints
# ---------------------------------------------------------------------------
app_router = FastAPI(title="ADMS Receiver")


@app_router.get("/iclock/cdata", response_class=PlainTextResponse)
async def device_handshake(
    SN: str = Query(...),
    options: str = Query(None),
    pushver: str = Query(None),
):
    register_device(SN)
    log.info(f"[{SN}] Handshake (pushver={pushver})")
    config = (
        f"GET OPTION FROM: {SN}\r\n"
        f"ATTLOGStamp=0\r\n"
        f"OPERLOGStamp=9999\r\n"
        f"BIODATAStamp=0\r\n"
        f"FPTrans=1\r\n"
        f"ErrorDelay=30\r\n"
        f"Delay=10\r\n"
        f"TransTimes=09:00;14:00;17:00;21:00\r\n"
        f"TransInterval=1\r\n"
        f"TransFlag=TransData AttLog FPTrans\r\n"
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
    elif table in ("BIODATA", "FP", "FPTRANSACTION"):
        count = store_biodata(SN, raw)
        if count == 0:
            log.warning(f"[{SN}] Terima {table} tapi 0 baris berhasil diparse.")
    else:
        log.info(f"[{SN}] Tabel tidak dikenal: {table} (Stamp={Stamp})")

    return PlainTextResponse("OK")


@app_router.get("/iclock/getrequest", response_class=PlainTextResponse)
async def get_request(SN: str = Query(...), INFO: str = Query(None)):
    register_device(SN)
    row = db_one("""
        SELECT id, command_text FROM adms_command_queue
        WHERE target_device = %s AND sent_at IS NULL
        ORDER BY id ASC
        LIMIT 1
    """, (SN,))
    if row:
        db_run("UPDATE adms_command_queue SET sent_at = NOW() WHERE id = %s", (row["id"],))
        log.info(f"[{SN}] Kirim command #{row['id']}: {row['command_text'][:80]}")
        return PlainTextResponse(f"C:{row['id']}:{row['command_text']}")
    return PlainTextResponse("OK")


@app_router.post("/iclock/devicecmd", response_class=PlainTextResponse)
async def device_cmd(request: Request, SN: str = Query(...)):
    body = (await request.body()).decode("utf-8", errors="ignore")
    body_params: dict[str, str] = {}
    for part in body.strip().split("&"):
        if "=" in part:
            k, _, v = part.partition("=")
            body_params[k.strip()] = v.strip()

    cmd_id = body_params.get("ID") or dict(request.query_params).get("ID")
    return_val = body_params.get("Return", "0")

    if cmd_id:
        try:
            ret = int(return_val)
            db_run("UPDATE adms_command_queue SET acked_at = NOW() WHERE id = %s", (int(cmd_id),))
            if ret != 0:
                log.warning(f"[{SN}] Command #{cmd_id} gagal di mesin (Return={ret}).")
        except Exception as exc:
            log.warning(f"[{SN}] devicecmd error: {exc}")

    return PlainTextResponse("OK")


# ---------------------------------------------------------------------------
# Management endpoints
# ---------------------------------------------------------------------------
@app_router.get("/health")
async def health():
    devices = get_all_device_sns()
    pending = db_one("SELECT COUNT(*) as n FROM adms_command_queue WHERE sent_at IS NULL")["n"]
    fp_count = db_one("SELECT COUNT(*) as n FROM adms_biodata")["n"]
    return {
        "status": "ok",
        "time": datetime.now(WIB).isoformat(),
        "devices": devices,
        "pending_commands": pending,
        "fingerprints_stored": fp_count,
    }


@app_router.get("/devices")
async def list_devices():
    rows = db_all("SELECT sn, last_seen FROM adms_devices ORDER BY last_seen DESC")
    pending_rows = db_all("""
        SELECT target_device, COUNT(*) as n
        FROM adms_command_queue WHERE sent_at IS NULL
        GROUP BY target_device
    """)
    pending_map = {r["target_device"]: r["n"] for r in pending_rows}
    return {
        "devices": [
            {
                "sn": r["sn"],
                "last_seen": str(r["last_seen"]),
                "pending_commands": pending_map.get(r["sn"], 0),
            }
            for r in rows
        ]
    }


@app_router.get("/inject-employees")
async def inject_employees(force: int = Query(0)):
    """Antrekan karyawan aktif ke semua mesin.

    Secara default hanya inject karyawan BARU yang belum pernah dikirim ke mesin.
    Tambahkan ?force=1 untuk inject ulang semua (gunakan jika mesin di-reset/baru).
    """
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
        return {"error": "Belum ada mesin terdaftar. Pastikan mesin sudah terhubung ke server."}

    # Hapus command DATA USER yang belum terkirim (akan di-queue ulang jika perlu)
    db_run("DELETE FROM adms_command_queue WHERE sent_at IS NULL AND command_text LIKE 'DATA USER%'")

    statements = []
    skipped_per_device: dict[str, int] = {}

    for sn in device_sns:
        if force:
            already_sent: set[str] = set()
        else:
            # Ambil employee codes yang sudah pernah terkirim ke mesin ini
            rows = db_all("""
                SELECT command_text FROM adms_command_queue
                WHERE target_device = %s
                  AND sent_at IS NOT NULL
                  AND command_text LIKE 'DATA USER PIN=%'
            """, (sn,))
            already_sent: set[str] = set()
            for r in rows:
                # Ekstrak PIN dari "DATA USER PIN=0067\tName=..."
                txt: str = r["command_text"]
                try:
                    pin_part = txt.split("\t")[0]  # "DATA USER PIN=0067"
                    pin = pin_part.split("PIN=")[1]
                    already_sent.add(pin)
                except IndexError:
                    pass

        skipped = 0
        for emp in emp_list:
            code = emp["employeeCode"]
            if code in already_sent:
                skipped += 1
                continue
            cmd = (
                f"DATA USER PIN={code}\t"
                f"Name={emp['fullName'][:24]}\t"
                f"Pri=0\tPasswd=\tCard=\tGrp=1\tTZ=0000000100000000\tVerify=0"
            )
            statements.append((
                "INSERT INTO adms_command_queue (target_device, command_text) VALUES (%s, %s)",
                (sn, cmd),
            ))
        skipped_per_device[sn] = skipped

    if statements:
        db_run_many(statements)

    total = len(statements)
    log.info(f"inject-employees: {total} command baru, skip={skipped_per_device} (force={bool(force)})")
    return {
        "status": "ok",
        "total_queued": total,
        "skipped_existing": skipped_per_device,
        "devices": device_sns,
        "tip": "Gunakan ?force=1 jika mesin di-reset dan perlu inject ulang semua karyawan.",
    }


@app_router.get("/inject-fingerprints")
async def inject_fingerprints(target: str = Query(None)):
    """Antrekan semua fingerprint tersimpan ke semua mesin (atau ?target=SN untuk satu mesin)."""
    templates = db_all(
        "SELECT employee_code, finger_id, template_size, template_data FROM adms_biodata WHERE valid = 1"
    )
    if not templates:
        return {"status": "ok", "queued": 0, "message": "Belum ada fingerprint tersimpan."}

    device_sns = [target] if target else get_all_device_sns()
    if not device_sns:
        return {"error": "Belum ada mesin terdaftar."}

    if target:
        db_run(
            "DELETE FROM adms_command_queue WHERE target_device = %s AND sent_at IS NULL AND command_text LIKE 'DATA FP%'",
            (target,),
        )
    else:
        db_run("DELETE FROM adms_command_queue WHERE sent_at IS NULL AND command_text LIKE 'DATA FP%'")

    statements = []
    for sn in device_sns:
        for t in templates:
            cmd = (
                f"DATA FP PIN={t['employee_code']}\t"
                f"FID={t['finger_id']}\t"
                f"Size={t['template_size']}\tValid=1\tTEMPLATE={t['template_data']}"
            )
            statements.append((
                "INSERT INTO adms_command_queue (target_device, command_text) VALUES (%s, %s)",
                (sn, cmd),
            ))
    db_run_many(statements)

    total = len(templates) * len(device_sns)
    log.info(f"inject-fingerprints: {len(templates)} template × {len(device_sns)} mesin = {total} command.")
    return {
        "status": "ok",
        "fingerprints": len(templates),
        "devices": device_sns,
        "total_queued": total,
    }


@app_router.get("/sync/{target_date}")
async def trigger_sync(target_date: str):
    """Kirim ulang data absensi tanggal tertentu ke HRD Dashboard."""
    try:
        d = date.fromisoformat(target_date)
    except ValueError:
        return {"error": "Format tanggal tidak valid. Gunakan YYYY-MM-DD."}
    send_batch_for_date(d)
    return {"status": "ok", "synced_date": target_date}


@app_router.get("/sync-now")
async def trigger_sync_now():
    """Kirim data absensi hari ini dan kemarin ke HRD Dashboard sekarang."""
    scheduled_sync()
    return {"status": "ok", "time": datetime.now(WIB).isoformat()}


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL env var wajib diset!")
    log.info("Koneksi ke Supabase PostgreSQL...")
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
