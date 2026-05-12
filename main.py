#!/usr/bin/env python3
"""
ADMS Receiver Server — ZKTeco AT301
Data disimpan di Supabase PostgreSQL (bukan SQLite lokal).

Env vars yang wajib diset di Coolify:
  DATABASE_URL       — postgres connection string Supabase (port 5433 PgBouncer)
  ADMS_INGEST_TOKEN  — token bearer untuk HRD Dashboard API
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
def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn


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
    """Jalankan banyak (sql, params) dalam satu transaksi."""
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
    rows = db_all("SELECT sn FROM adms_devices")
    return [r["sn"] for r in rows]


# ---------------------------------------------------------------------------
# Fingerprint sync
# ---------------------------------------------------------------------------
def store_biodata(device_sn: str, raw_body: str):
    new_templates: list[tuple] = []
    statements = []

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

        new_templates.append((employee_code, finger_id, template_size, template_data))

    if statements:
        db_run_many(statements)
        log.info(f"[{device_sn}] Tersimpan {len(new_templates)} fingerprint template.")
        _queue_fingerprints_to_other_devices(device_sn, new_templates)


def store_querydata_biodata(device_sn: str, raw_body: str) -> int:
    """Parse format key=value dari /iclock/querydata (respon DATA QUERY biodata).

    Tiap baris: Pin=0067\tFid=0\tSize=498\tValid=1\ttmp=BASE64...
    Fieldname bisa bervariasi tergantung firmware (Type vs Fid, tmp vs Template).
    """
    new_templates: list[tuple] = []
    statements = []

    for line in raw_body.strip().splitlines():
        fields: dict[str, str] = {}
        for part in line.strip().split("\t"):
            if "=" in part:
                k, _, v = part.partition("=")
                fields[k.strip().lower()] = v.strip()

        employee_code = fields.get("pin")
        finger_id_str = fields.get("fid") or fields.get("type")
        template_data = fields.get("tmp") or fields.get("template")

        if not employee_code or finger_id_str is None or not template_data:
            log.debug(f"[{device_sn}] querydata baris dilewati: {line[:80]!r}")
            continue

        try:
            finger_id = int(finger_id_str)
        except ValueError:
            continue

        try:
            valid = int(fields.get("valid", "1"))
        except ValueError:
            valid = 1

        try:
            template_size = int(fields.get("size", "0")) or len(template_data)
        except ValueError:
            template_size = len(template_data)

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

        new_templates.append((employee_code, finger_id, template_size, template_data))

    if statements:
        db_run_many(statements)
        log.info(f"[{device_sn}] querydata: {len(new_templates)} fingerprint disimpan.")
        _queue_fingerprints_to_other_devices(device_sn, new_templates)

    return len(statements)


def _queue_fingerprints_to_other_devices(source_sn: str, templates: list[tuple]):
    other_sns = [sn for sn in get_all_device_sns() if sn != source_sn]
    if not other_sns:
        return

    statements = []
    for target_sn in other_sns:
        for emp_code, finger_id, template_size, template_data in templates:
            cmd = (
                f"DATA FP PIN={emp_code}\tFID={finger_id}"
                f"\tSize={template_size}\tValid=1\tTEMPLATE={template_data}"
            )
            statements.append((
                "INSERT INTO adms_command_queue (target_device, command_text) VALUES (%s, %s)",
                (target_sn, cmd),
            ))
    db_run_many(statements)
    log.info(f"Queued {len(templates)} fingerprint(s) ke {len(other_sns)} mesin lain.")


# ---------------------------------------------------------------------------
# Punch log
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
        log.info(f"[{device_sn}] Tersimpan {len(statements)} punch baru.")


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

    rows = db_all("""
        SELECT device_sn, employee_code, punch_time, punch_type
        FROM adms_punch_logs
        WHERE punch_time::date = %s
        ORDER BY punch_time ASC
    """, (date_str,))

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
        record: dict = {"employeeCode": emp_code, "attendanceDate": date_str, "attendanceStatus": "HADIR"}
        if g["check_in"]:  record["checkInTime"]  = g["check_in"]
        if g["check_out"]: record["checkOutTime"] = g["check_out"]
        if g["break_out"]: record["breakOutTime"] = g["break_out"]
        if g["break_in"]:  record["breakInTime"]  = g["break_in"]
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
            for err in (result.get("errors") or [])[:5]:
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
async def device_handshake(SN: str = Query(...), options: str = Query(None), pushver: str = Query(None)):
    register_device(SN)
    log.info(f"[{SN}] Handshake dari mesin.")
    # FPTrans=1 diperlukan agar device mengizinkan DATA QUERY biodata (Return=-3 jika tidak ada).
    # BIODATAStamp=0 → jika device mendukung auto-push biodata, mulai dari awal.
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
async def receive_data(request: Request, SN: str = Query(...), table: str = Query(None), Stamp: str = Query(None)):
    body = await request.body()
    raw = body.decode("utf-8", errors="ignore")
    if table == "ATTLOG":
        store_punches(SN, raw)
    elif table in ("BIODATA", "FP", "FPTRANSACTION", "templatev10"):
        log.info(f"[{SN}] Menerima tabel biometrik via cdata: table={table} ({len(raw)} bytes)")
        log.info(f"[{SN}] cdata biodata sample: {raw[:300]!r}")
        # Coba kedua parser — querydata format (key=value) lebih umum di fw8.x
        count = store_querydata_biodata(SN, raw)
        if count == 0:
            count = len([l for l in raw.splitlines() if l.strip()])
            store_biodata(SN, raw)
    else:
        log.info(f"[{SN}] Tabel tidak dikenal: table={table} Stamp={Stamp} body={raw[:300]!r}")
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
        db_run(
            "UPDATE adms_command_queue SET sent_at = NOW() WHERE id = %s",
            (row["id"],),
        )
        log.info(f"[{SN}] Mengirim command #{row['id']}: {row['command_text'][:60]}...")
        return PlainTextResponse(f"C:{row['id']}:{row['command_text']}")
    return PlainTextResponse("OK")


@app_router.post("/iclock/devicecmd", response_class=PlainTextResponse)
async def device_cmd(request: Request, SN: str = Query(...)):
    # Device mengirim ID & Return di BODY (form-encoded), bukan query params
    body = (await request.body()).decode("utf-8", errors="ignore")
    body_params: dict[str, str] = {}
    for part in body.strip().split("&"):
        if "=" in part:
            k, _, v = part.partition("=")
            body_params[k.strip()] = v.strip()

    # Fallback ke query params jika tidak ada di body
    qp = dict(request.query_params)
    cmd_id = body_params.get("ID") or qp.get("ID")
    return_val = body_params.get("Return") or qp.get("Return")
    cmd_type = body_params.get("CMD") or qp.get("CMD", "")

    log.info(f"[{SN}] devicecmd ID={cmd_id} Return={return_val} CMD={cmd_type!r}")

    if cmd_id:
        try:
            db_run(
                "UPDATE adms_command_queue SET acked_at = NOW() WHERE id = %s",
                (int(cmd_id),),
            )
            ret_int = int(return_val or "0")
            if ret_int == 0:
                log.info(f"[{SN}] Command #{cmd_id} berhasil (Return=0)")
            else:
                log.warning(f"[{SN}] Command #{cmd_id} GAGAL (Return={ret_int}): CMD={cmd_type!r}")
        except Exception as exc:
            log.warning(f"[{SN}] Gagal proses devicecmd #{cmd_id}: {exc}")
    else:
        log.warning(f"[{SN}] devicecmd tanpa ID — body={body[:200]!r}")
    return PlainTextResponse("OK")


@app_router.post("/iclock/querydata", response_class=PlainTextResponse)
async def receive_querydata(
    request: Request,
    SN: str = Query(...),
    type: str = Query(None),
    cmdid: str = Query(None),
):
    """Endpoint yang dipanggil mesin setelah menerima command C:ID:DATA QUERY biodata.

    Device mengirim fingerprint templates satu per baris (key=value tab-separated).
    """
    body = await request.body()
    raw = body.decode("utf-8", errors="ignore")
    log.info(f"[{SN}] querydata type={type} cmdid={cmdid} ({len(raw)} bytes)")

    if type == "biodata":
        count = store_querydata_biodata(SN, raw)
        log.info(f"[{SN}] Total {count} template diproses dari querydata.")
    else:
        log.info(f"[{SN}] querydata type tidak dikenal: {type!r} — body: {raw[:200]!r}")

    if cmdid:
        try:
            db_run(
                "UPDATE adms_command_queue SET acked_at = NOW() WHERE id = %s",
                (int(cmdid),),
            )
        except Exception as exc:
            log.warning(f"[{SN}] Gagal ack querydata cmd {cmdid}: {exc}")

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


@app_router.get("/inject-employees")
async def inject_employees():
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

    db_run("DELETE FROM adms_command_queue WHERE sent_at IS NULL AND command_text LIKE 'DATA USER%'")

    statements = []
    for sn in device_sns:
        for emp in emp_list:
            pin = emp["employeeCode"]
            name = emp["fullName"][:24]
            cmd = f"DATA USER PIN={pin}\tName={name}\tPri=0\tPasswd=\tCard=\tGrp=1\tTZ=0000000100000000\tVerify=0"
            statements.append((
                "INSERT INTO adms_command_queue (target_device, command_text) VALUES (%s, %s)",
                (sn, cmd),
            ))
    db_run_many(statements)

    total = len(emp_list) * len(device_sns)
    log.info(f"Inject: {len(emp_list)} karyawan × {len(device_sns)} mesin = {total} command.")
    return {"status": "ok", "employees": len(emp_list), "devices": device_sns, "total_queued": total}


@app_router.get("/inject-fingerprints")
async def inject_fingerprints(target: str = Query(None)):
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
                f"DATA FP PIN={t['employee_code']}\tFID={t['finger_id']}"
                f"\tSize={t['template_size']}\tValid=1\tTEMPLATE={t['template_data']}"
            )
            statements.append((
                "INSERT INTO adms_command_queue (target_device, command_text) VALUES (%s, %s)",
                (sn, cmd),
            ))
    db_run_many(statements)

    total = len(templates) * len(device_sns)
    log.info(f"Inject fingerprint: {len(templates)} template × {len(device_sns)} mesin = {total} command.")
    return {"status": "ok", "fingerprints": len(templates), "devices": device_sns, "total_queued": total}


@app_router.get("/pull-fingerprints")
async def pull_fingerprints(sn: str = Query(None)):
    """Antrekan command DATA QUERY untuk menarik fingerprint dari mesin.

    Coba dua table name: 'biodata' (fw8.x unified) dan 'templatev10' (legacy).
    Device merespons ke /iclock/querydata atau /iclock/cdata tergantung firmware.
    """
    device_sns = [sn] if sn else get_all_device_sns()
    if not device_sns:
        return {"error": "Belum ada mesin terdaftar. Pastikan mesin sudah handshake ke server."}

    statements = []
    for target_sn in device_sns:
        statements.append((
            "INSERT INTO adms_command_queue (target_device, command_text) VALUES (%s, %s)",
            (target_sn, "DATA QUERY biodata"),
        ))
    db_run_many(statements)

    log.info(f"Queued DATA QUERY biodata ke {len(device_sns)} mesin: {device_sns}")
    return {
        "status": "ok",
        "devices": device_sns,
        "queued_command": "DATA QUERY biodata",
        "note": "Mesin perlu handshake dulu dengan config FPTrans=1 sebelum QUERY berhasil.",
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
            {"sn": r["sn"], "last_seen": str(r["last_seen"]), "pending_commands": pending_map.get(r["sn"], 0)}
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
