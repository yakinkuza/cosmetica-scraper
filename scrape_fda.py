import os
import json
import time
import requests

import gspread
from gspread.exceptions import WorksheetNotFound

# -----------------------------
# Config
# -----------------------------
URL = "https://cosmetica.fda.moph.go.th/CMT_SEARCH_BACK_NEW/Home/FUNCTION_CENTER"

# ID ของ Google Sheet (ตัวใหม่ที่คุณบีมให้มา)
SHEET_ID = os.getenv(
    "SHEET_ID",
    "1sEwh39a_C_jcYXBPbkU6tN_nWUmp7_juEQkBy7gcoxM",
)

# ชื่อแท็บใน Google Sheet
LIST_SHEET_NAME = os.getenv("LIST_SHEET_NAME", "LIST")
RESULT_SHEET_NAME = os.getenv("RESULT_SHEET_NAME", "RESULT")
ERROR_SHEET_NAME = os.getenv("ERROR_SHEET_NAME", "ERROR")

# จำนวน record สูงสุดต่อการรัน 1 ครั้ง (กันไม่ให้ run นานเกินไป)
MAX_PER_RUN = int(os.getenv("MAX_PER_RUN", "500"))

# batch ที่จะเขียนกลับเข้า Google Sheet ทีละกี่แถว
BATCH_WRITE_SIZE = int(os.getenv("BATCH_WRITE_SIZE", "50"))

# -----------------------------
# Helper: connect gspread
# -----------------------------
def get_gspread_client():
    """
    ใช้ GOOGLE_SERVICE_ACCOUNT_JSON จาก GitHub Secret
    เพื่อสร้าง gspread client
    """
    creds_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds_dict = json.loads(creds_json)
    return gspread.service_account_from_dict(creds_dict)


# -----------------------------
# FDA API
# -----------------------------
def build_payload(regnos: str) -> dict:
    """
    สร้าง payload สำหรับเรียก get_detail_regnos
    regnos ต้องเป็นเลขแบบไม่มีขีด เช่น "1026700038284"
    """
    return {
        "MODEL": {
            "M_SYSTEM_SETTING": {"FUNCTION_NAME": "get_detail_regnos"},
            "M_AUTHENTICATION": {},
            "DATA_SET": {},
            "DATA_TRANSLATION_OB": None,
            "Search": {},
            "datail_string": {
                "regnos": regnos,  # สำคัญสุด
                # field อื่นปล่อยว่าง ระบบจะเติมให้เอง
            },
            "M_tran": {},
        }
    }


def call_fda(regnos: str, retry: int = 3, sleep_sec: float = 1.0) -> dict | None:
    """
    เรียก API FDA ด้วย regnos (ไม่มีขีด) แล้วคืนค่า datail_string (dict)
    ถ้า error หลายครั้ง คืน None
    """
    payload = build_payload(regnos)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
    }

    for attempt in range(1, retry + 1):
        try:
            r = requests.post(URL, headers=headers, data=json.dumps(payload), timeout=30)
            r.raise_for_status()
            data = r.json()

            model = data.get("MODEL", data)
            detail = model.get("datail_string", None)

            # ถ้าได้ detail กลับมา ก็จบ
            if detail:
                return detail

            # ถ้า detail ว่าง ลอง print ดูเพื่อ debug
            print(f"  ⚠ regnos {regnos}: datail_string is empty")
            return None

        except Exception as e:
            print(f"  ❌ regnos {regnos}: attempt {attempt} failed -> {e}")
            if attempt < retry:
                time.sleep(sleep_sec * attempt)
            else:
                return None


# -----------------------------
# Helper: flatten detail → row
# -----------------------------
DETAIL_KEYS = [
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


def normalize_value(v):
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def detail_to_row(notify_number: str, detail: dict) -> list:
    """
    แปลง dict datail_string → list สำหรับเขียนลง Google Sheet
    column แรกคือ notify_number แบบมีขีด
    """
    row = [notify_number]
    for key in DETAIL_KEYS:
        row.append(normalize_value(detail.get(key)))
    return row


def get_result_header() -> list:
    return ["notify_number"] + DETAIL_KEYS


# -----------------------------
# Main
# -----------------------------
def main():
    print("🚀 Start scraping")

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)

    # --- เตรียม worksheet ต่าง ๆ ---
    ws_list = sh.worksheet(LIST_SHEET_NAME)

    try:
        ws_result = sh.worksheet(RESULT_SHEET_NAME)
    except WorksheetNotFound:
        ws_result = sh.add_worksheet(RESULT_SHEET_NAME, rows=1000, cols=50)
        ws_result.append_row(get_result_header())
        print(f"✅ Created sheet '{RESULT_SHEET_NAME}' with header")

    try:
        ws_error = sh.worksheet(ERROR_SHEET_NAME)
    except WorksheetNotFound:
        ws_error = sh.add_worksheet(ERROR_SHEET_NAME, rows=1000, cols=10)
        ws_error.append_row(["notify_number", "regnos_no_dash", "error_message"])
        print(f"✅ Created sheet '{ERROR_SHEET_NAME}' with header")

    # --- โหลดเลขทั้งหมดจาก LIST ---
    all_vals = ws_list.col_values(1)  # คอลัมน์ A ทั้งหมด
    if not all_vals:
        print("❌ LIST sheet col A ว่างเปล่า")
        return

    # แยก header + list จริง
    if all_vals[0].strip().lower().startswith("notify"):
        notify_all = [v.strip() for v in all_vals[1:] if v.strip()]
    else:
        notify_all = [v.strip() for v in all_vals if v.strip()]

    print(f"🔢 Total notify numbers in LIST: {len(notify_all)}")

    # --- โหลด notify ที่เคยดึงไปแล้วจาก RESULT (เลี่ยงซ้ำ) ---
    result_vals = ws_result.col_values(1)  # notify_number อยู่คอลัมน์แรก
    if result_vals and result_vals[0] == "notify_number":
        done_set = set(v.strip() for v in result_vals[1:] if v.strip())
    else:
        done_set = set(v.strip() for v in result_vals if v.strip())

    print(f"✅ Already scraped: {len(done_set)}")

    # เลือกเฉพาะที่ยังไม่เคยดึง
    to_scrape_all = [n for n in notify_all if n not in done_set]

    if not to_scrape_all:
        print("🎉 All records already scraped. Nothing to do.")
        return

    # จำกัดจำนวนต่อการรัน
    to_scrape = to_scrape_all[:MAX_PER_RUN]
    print(f"🧮 This run will scrape: {len(to_scrape)} records")

    # --- Loop ดึงข้อมูล ---
    batch_rows = []
    error_rows = []

    for idx, notify in enumerate(to_scrape, start=1):
        regnos = notify.replace("-", "")
        print(f"[{idx}/{len(to_scrape)}] {notify} -> {regnos}")

        detail = call_fda(regnos)

        if detail:
            row = detail_to_row(notify, detail)
            batch_rows.append(row)
        else:
            error_rows.append(
                [notify, regnos, "No data or API error (see logs)"]
            )

        # เขียนเป็น batch ลง RESULT ทุก ๆ BATCH_WRITE_SIZE แถว
        if len(batch_rows) >= BATCH_WRITE_SIZE:
            ws_result.append_rows(batch_rows, value_input_option="RAW")
            print(f"  💾 Wrote {len(batch_rows)} rows to RESULT")
            batch_rows = []

        # เขียน error เป็น batch
        if len(error_rows) >= BATCH_WRITE_SIZE:
            ws_error.append_rows(error_rows, value_input_option="RAW")
            print(f"  💾 Wrote {len(error_rows)} rows to ERROR")
            error_rows = []

        # กัน API โดน spam จน block (ปรับได้ตามจริง)
        time.sleep(0.2)

    # เขียนเศษ batch ที่เหลือ
    if batch_rows:
        ws_result.append_rows(batch_rows, value_input_option="RAW")
        print(f"💾 Wrote final {len(batch_rows)} rows to RESULT")

    if error_rows:
        ws_error.append_rows(error_rows, value_input_option="RAW")
        print(f"💾 Wrote final {len(error_rows)} rows to ERROR")

    print("✅ Done this run")


if __name__ == "__main__":
    main()
