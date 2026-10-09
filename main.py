#!/usr/bin/env python3
"""
ADMS Receiver — ZKTeco AT301
Menerima data absensi dari mesin, kirim ke HRD Dashboard.

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
ADMS_INGEST_TOKEN = os.environ.get("ADMS_INGEST_TOKEN", "").strip().strip("\"'").strip()

_parsed = urlparse(HRD_API_URL)
HRD_EMPLOYEES_URL = f"{_parsed.scheme}://{_parsed.netloc}/api/integrations/adms/employees"
HRD_TAPS_URL      = f"{_parsed.scheme}://{_parsed.netloc}/api/integrations/adms/taps"

# HTTP session dengan custom User-Agent browser agar tidak diblokir Cloudflare WAF bot filter
http_session = requests.Session()
http_session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 BioFinger-ADMS/1.0",
    "Accept": "application/json",
})

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
    return psycopg2.connect(
        DATABASE_URL,
        options="-c timezone=Asia/Jakarta",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


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
def send_batch_for_date(target_date: date) -> dict:
    """Kirim semua tap mentah hari target_date ke HRD Dashboard.

    HRD Dashboard yang mengklasifikasikan tiap tap (check_in / break_out /
    break_in / check_out) berdasarkan jadwal kerja masing-masing karyawan.
    punch_type dari mesin diabaikan sepenuhnya — jam tap yang menentukan.
    """
    if not ADMS_INGEST_TOKEN:
        log.warning("ADMS_INGEST_TOKEN belum diset, batch dilewati.")
        return {"status": "error", "error": "ADMS_INGEST_TOKEN belum diset di service ADMS."}

    date_str = target_date.isoformat()
    rows = db_all("""
        SELECT device_sn, employee_code, punch_time
        FROM adms_punch_logs
        WHERE punch_time::date = %s
        ORDER BY punch_time ASC
    """, (date_str,))

    if not rows:
        log.info(f"[{date_str}] Tidak ada data punch.")
        return {"status": "ok", "date": date_str, "total_punches": 0, "message": "Tidak ada data punch."}

    # Gabungkan tap dari semua device; deduplikasi per (employee_code, HH:MM).
    seen: set[tuple] = set()
    taps: list[dict] = []
    for row in rows:
        t = row["punch_time"].strftime("%H:%M")
        key = (row["employee_code"], t)
        if key in seen:
            continue
        seen.add(key)
        taps.append({
            "employeeCode": row["employee_code"],
            "time": t,
            "deviceId": row["device_sn"],
        })

    try:
        resp = http_session.post(
            HRD_TAPS_URL,
            headers={
                "Authorization": f"Bearer {ADMS_INGEST_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"date": date_str, "taps": taps},
            timeout=30,
            allow_redirects=False,
        )
        resp.raise_for_status()
        result = resp.json()
        log.info(
            f"[{date_str}] taps={len(taps)} karyawan={result.get('total', 0)} "
            f"inserted:{result.get('inserted', 0)} "
            f"updated:{result.get('updated', 0)} "
            f"skipped:{result.get('skipped', 0)}"
        )
        for err in (result.get("errors") or [])[:5]:
            log.warning(f"  skip [{err.get('employeeCode')}]: {err.get('reason')}")
        return {
            "status": "ok",
            "date": date_str,
            "total_punches": len(rows),
            "unique_taps": len(taps),
            "hris_result": result,
        }
    except Exception as exc:
        log.error(f"Gagal kirim taps {date_str}: {exc}")
        return {
            "status": "error",
            "date": date_str,
            "total_punches": len(rows),
            "unique_taps": len(taps),
            "error": str(exc),
        }


def scheduled_sync() -> dict:
    today = datetime.now(WIB).date()
    log.info(f"=== Scheduled sync: {today} ===")
    res_today = send_batch_for_date(today)
    res_yesterday = send_batch_for_date(today - timedelta(days=1))
    return {
        "today": res_today,
        "yesterday": res_yesterday,
    }


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

    # Pastikan mesin disinkronkan jamnya ke WIB jika belum ada antrean waktu
    has_time_cmd = db_one(
        "SELECT 1 FROM adms_command_queue WHERE target_device = %s AND sent_at IS NULL AND command_text LIKE %s",
        (SN, "SET OPTIONS DateTime%"),
    )
    if not has_time_cmd:
        now_wib = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
        db_run(
            "INSERT INTO adms_command_queue (target_device, command_text) VALUES (%s, %s)",
            (SN, f"SET OPTIONS DateTime={now_wib}"),
        )
        log.info(f"[{SN}] Auto-queue sinkronisasi waktu mesin: {now_wib} WIB")

    config = (
        f"GET OPTION FROM: {SN}\r\n"
        f"ATTLOGStamp=0\r\n"
        f"OPERLOGStamp=9999\r\n"
        f"ErrorDelay=30\r\n"
        f"Delay=10\r\n"
        f"TransTimes=09:00;14:00;17:00;21:00\r\n"
        f"TransInterval=1\r\n"
        f"TransFlag=TransData AttLog\r\n"
        f"TimeZone=7\r\n"
        f"Realtime=1\r\n"
        f"Encrypt=None\r\n"
        f"ServerVer=3.4.1\r\n"
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
    else:
        log.info(f"[{SN}] Tabel diabaikan: {table} (Stamp={Stamp})")

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
    punch_today = db_one("""
        SELECT COUNT(*) as n FROM adms_punch_logs
        WHERE punch_time::date = CURRENT_DATE
    """)["n"]
    return {
        "status": "ok",
        "time": datetime.now(WIB).isoformat(),
        "devices": devices,
        "pending_commands": pending,
        "punches_today": punch_today,
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
    """Antrekan karyawan aktif dari HRD Dashboard ke semua mesin.

    Default: hanya inject karyawan BARU yang belum pernah dikirim ke mesin.
    Gunakan ?force=1 jika mesin di-reset atau mesin baru ditambahkan.
    """
    if not ADMS_INGEST_TOKEN:
        return {"error": "ADMS_INGEST_TOKEN belum diset."}

    try:
        resp = http_session.get(
            HRD_EMPLOYEES_URL,
            headers={"Authorization": f"Bearer {ADMS_INGEST_TOKEN}"},
            timeout=30,
            allow_redirects=False,
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

    db_run(
        "DELETE FROM adms_command_queue WHERE sent_at IS NULL AND command_text LIKE %s",
        ("DATA USER%",),
    )

    statements = []
    skipped_per_device: dict[str, int] = {}

    for sn in device_sns:
        if force:
            already_sent: set[str] = set()
        else:
            rows = db_all("""
                SELECT command_text FROM adms_command_queue
                WHERE target_device = %s
                  AND sent_at IS NOT NULL
                  AND command_text LIKE %s
            """, (sn, "DATA USER PIN=%"))
            already_sent = set()
            for r in rows:
                try:
                    pin = r["command_text"].split("\t")[0].split("PIN=")[1]
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
    log.info(f"inject-employees: {total} baru, skip={skipped_per_device} (force={bool(force)})")
    return {
        "status": "ok",
        "total_queued": total,
        "skipped_existing": skipped_per_device,
        "devices": device_sns,
        "tip": "Gunakan ?force=1 jika mesin di-reset dan perlu inject ulang semua karyawan.",
    }


@app_router.get("/sync/{target_date}")
async def trigger_sync(target_date: str):
    """Kirim ulang data absensi tanggal tertentu ke HRD Dashboard."""
    try:
        d = date.fromisoformat(target_date)
    except ValueError:
        return {"error": "Format tanggal tidak valid. Gunakan YYYY-MM-DD."}
    res = send_batch_for_date(d)
    return {"status": res.get("status", "ok"), "synced_date": target_date, "detail": res}


@app_router.get("/sync-now")
async def trigger_sync_now():
    """Kirim data absensi hari ini dan kemarin ke HRD Dashboard sekarang."""
    sync_results = scheduled_sync()
    has_error = (
        sync_results.get("today", {}).get("status") == "error"
        or sync_results.get("yesterday", {}).get("status") == "error"
    )
    return {
        "status": "error" if has_error else "ok",
        "time": datetime.now(WIB).isoformat(),
        **sync_results,
    }


@app_router.get("/sync-time")
async def sync_device_time():
    """Kirim perintah sinkronisasi jam mesin ke waktu WIB (Asia/Jakarta, UTC+7)."""
    device_sns = get_all_device_sns()
    if not device_sns:
        return {"error": "Belum ada mesin terdaftar. Pastikan mesin sudah terhubung ke server."}

    now_wib = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
    cmd = f"SET OPTIONS DateTime={now_wib}"
    statements = [
        ("INSERT INTO adms_command_queue (target_device, command_text) VALUES (%s, %s)", (sn, cmd))
        for sn in device_sns
    ]
    db_run_many(statements)
    log.info(f"Antrekan sinkronisasi waktu mesin ({now_wib}) ke {len(device_sns)} mesin: {device_sns}")
    return {
        "status": "ok",
        "time_wib": now_wib,
        "devices": device_sns,
        "queued_command": cmd,
        "message": "Perintah sync jam mesin ke UTC+7 (WIB) telah diantrekan ke semua mesin.",
    }


@app_router.get("/clear-employees")
async def clear_all_employees(confirm: str = Query(None)):
    """Hapus seluruh user di semua mesin fingerprint via ADMS.
    Wajib tambahkan ?confirm=yes untuk konfirmasi penghapusan seluruh karyawan di mesin.
    """
    if confirm != "yes":
        return {"error": "Tambahkan ?confirm=yes untuk konfirmasi penghapusan seluruh karyawan di mesin."}

    device_sns = get_all_device_sns()
    if not device_sns:
        return {"error": "Belum ada mesin terdaftar."}

    # Hapus antrean user lama agar bersih
    db_run("DELETE FROM adms_command_queue WHERE command_text LIKE 'DATA USER%'")

    statements = [
        ("INSERT INTO adms_command_queue (target_device, command_text) VALUES (%s, %s)", (sn, "DATA DELETE user *"))
        for sn in device_sns
    ]
    db_run_many(statements)
    log.info(f"Antrekan hapus seluruh karyawan ke {len(device_sns)} mesin: {device_sns}")
    return {
        "status": "ok",
        "devices": device_sns,
        "message": "Perintah 'DATA DELETE user *' telah diantrekan ke semua mesin.",
    }


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
