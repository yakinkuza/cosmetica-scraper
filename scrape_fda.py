#!/usr/bin/env python
import os
import json
import time
from typing import Dict, Any, List

import requests
import gspread
from google.oauth2.service_account import Credentials
from tqdm import tqdm

# ===============================
# CONFIG
# ===============================
URL = "https://cosmetica.fda.moph.go.th/CMT_SEARCH_BACK_NEW/Home/FUNCTION_CENTER"

# เอา spreadsheet id จาก Secrets ถ้าไม่ได้ตั้งก็ fallback เป็น id ของไฟล์ที่ส่งมา
SPREADSHEET_ID = os.environ.get(
    "SPREADSHEET_ID",
    "1sEwh39a_C_jcYXBPbkU6tN_nWUmp7_juEQkBy7gcoxM",
)

INPUT_SHEET_NAME = "INPUT"   # ลิสต์เลขจดแจ้ง (มี header แถว 1)
RESULT_SHEET_NAME = "RESULT" # เก็บผลลัพธ์
ERROR_SHEET_NAME = "ERROR"   # เก็บ error

BATCH_SIZE = 100             # เซฟลง RESULT ทีละกี่รายการ
MAX_RETRIES = 3              # retry request ต่อเลขจดแจ้ง
TIMEOUT = 30                 # timeout ของ request (วินาที)

# list คอลัมน์ที่ต้องการจาก datail_string
DETAIL_COLUMNS = [
    "regnos",
    "type",
    "lb_lct_type",
    "EMPLOYER",
    "lb_NAME_EMPLOYER",
    "status_lct",
    "lb_format_regnos",
    "lb_trade_Tpop",
    "lb_trade_Tpop2",
    "lb_cosnm_Tpop",
    "lb_cosnm_Tpop2",
    "lb_appdate",
    "lb_fileattach_count",
    "lb_status",
    "lb_expdate",
    "lb_no_regnos",
    "lb_mode",
    "lb_applicability_name",
    "lb_condition",
    "lb_application_name",
    "lb_usernm_pop",
    "lb_locat_pop",
    "lb_fac_pop",
    "lb_NO_pop",
    "count_eng",
    "data_ampole",
    "file",
    "fileType",
    "province",
    "identify",
    "lctnmno",
    "physical_detail",
]

# ===============================
# GSPREAD HELPER
# ===============================

def get_gspread_client() -> gspread.Client:
    """สร้าง gspread client จาก GOOGLE_SERVICE_ACCOUNT_JSON (เก็บใน GitHub Secrets)"""
    creds_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc


def open_sheets(gc: gspread.Client):
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws_input = sh.worksheet(INPUT_SHEET_NAME)
    ws_result = sh.worksheet(RESULT_SHEET_NAME)
    ws_error = sh.worksheet(ERROR_SHEET_NAME)
    return ws_input, ws_result, ws_error


def init_result_header(ws_result: gspread.Worksheet) -> None:
    """ถ้า RESULT ยังไม่มี header ให้สร้าง"""
    values = ws_result.get_all_values()
    if not values:
        header = ["notify_number"] + DETAIL_COLUMNS
        ws_result.append_row(header, value_input_option="RAW")


def init_error_header(ws_error: gspread.Worksheet) -> None:
    values = ws_error.get_all_values()
    if not values:
        ws_error.append_row(
            ["notify_number", "regnos", "error_message"],
            value_input_option="RAW",
        )


def read_notify_numbers(ws_input: gspread.Worksheet) -> List[str]:
    """อ่านเลขจดแจ้งจากชีต INPUT คอลัมน์ A (ข้าม header)"""
    col = ws_input.col_values(1)
    # แถวแรก header
    return [v.strip() for v in col[1:] if v.strip()]


def read_done_notify_numbers(ws_result: gspread.Worksheet) -> set:
    """อ่านเลขจดแจ้งที่ทำเสร็จแล้วจาก RESULT (คอลัมน์ notify_number)"""
    values = ws_result.col_values(1)
    return set(v.strip() for v in values[1:] if v.strip())


# ===============================
# FDA API
# ===============================

def build_payload(regnos: str) -> Dict[str, Any]:
    return {
        "MODEL": {
            "M_SYSTEM_SETTING": {"FUNCTION_NAME": "get_detail_regnos"},
            "M_AUTHENTICATION": {},
            "DATA_SET": {},
            "DATA_TRANSLATION_OB": None,
            "Search": {},
            "datail_string": {
                "regnos": regnos,
            },
            "M_tran": {},
        }
    }


def fetch_detail(regnos: str) -> Dict[str, Any]:
    """เรียก API พร้อม retry"""
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
    }

    payload = build_payload(regnos)

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                URL,
                headers=headers,
                json=payload,
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            model = data.get("MODEL", data)
            detail = model.get("datail_string", {}) or {}
            return detail
        except Exception as exc:
            last_exc = exc
            # backoff นิดหน่อย
            time.sleep(3 * attempt)

    # ถ้าไม่สำเร็จซักรอบให้โยน exception กลับไป
    raise last_exc


def detail_to_row(notify_number: str, detail: Dict[str, Any]) -> List[Any]:
    """แปลง detail_string ให้เป็น list ตามคอลัมน์ที่กำหนด"""
    row = [notify_number]
    for col in DETAIL_COLUMNS:
        val = detail.get(col, "")
        # แปลง dict / list เป็น JSON string ถ้ามี
        if isinstance(val, (dict, list)):
            val = json.dumps(val, ensure_ascii=False)
        row.append(val)
    return row


# ===============================
# MAIN LOGIC
# ===============================

def main():
    gc = get_gspread_client()
    ws_input, ws_result, ws_error = open_sheets(gc)

    init_result_header(ws_result)
    init_error_header(ws_error)

    notify_numbers = read_notify_numbers(ws_input)
    done_numbers = read_done_notify_numbers(ws_result)

    # เลขที่ยังไม่ได้ทำ
    todo = [n for n in notify_numbers if n not in done_numbers]

    if not todo:
        print("All notify numbers are already processed.")
        return

    print(f"Total notify numbers: {len(notify_numbers)}")
    print(f"Already done       : {len(done_numbers)}")
    print(f"To do              : {len(todo)}")

    batch_rows: List[List[Any]] = []

    with tqdm(total=len(todo), desc="Scraping", unit="item") as pbar:
        for notify_number in todo:
            regnos = notify_number.replace("-", "")
            try:
                detail = fetch_detail(regnos)
                row = detail_to_row(notify_number, detail)
                batch_rows.append(row)
            except Exception as exc:
                # บันทึก error แยกต่างหาก
                ws_error.append_row(
                    [notify_number, regnos, str(exc)],
                    value_input_option="RAW",
                )

            # flush ทีละชุด
            if len(batch_rows) >= BATCH_SIZE:
                ws_result.append_rows(batch_rows, value_input_option="RAW")
                batch_rows = []

            pbar.update(1)

        # เหลือชุดสุดท้าย
        if batch_rows:
            ws_result.append_rows(batch_rows, value_input_option="RAW")

    print("Done scraping.")


if __name__ == "__main__":
    main()
