import os
import json
import gspread
from google.oauth2.service_account import Credentials

# 1) โหลด service account จาก secret
creds_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
gc = gspread.authorize(creds)

# 2) ใช้ spreadsheet ID ที่ถูกต้อง
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "19ciuRoIOKVe3Rdrzi7HBAw_Sq_bEebwu")

sh = gc.open_by_key(SPREADSHEET_ID)
ws_result = sh.worksheet("RESULT")   # ต้องมีแท็บชื่อ RESULT

# แท็บผลลัพธ์ (สร้างชื่อแท็บนี้ไว้ก่อนใน Google Sheets เช่น 'RESULT')
ws_result = gc.open_by_key(SPREADSHEET_ID).worksheet("RESULT")

# แท็บที่มีเลขจดแจ้ง (เช่น 'notify_numbers')
ws_source = gc.open_by_key(SPREADSHEET_ID).worksheet("notify_numbers")

# อ่านเลขจดแจ้งจากคอลัมน์ A (หัวตาราง row1 เป็น header)
notify_numbers = [row[0] for row in ws_source.get_all_values()[1:] if row]

print(f"Total notify numbers: {len(notify_numbers)}")

def get_detail_regnos(regnos: str):
    payload = {
        "MODEL": {
            "M_SYSTEM_SETTING": {"FUNCTION_NAME": "get_detail_regnos"},
            "M_AUTHENTICATION": {},
            "DATA_SET": {},
            "DATA_TRANSLATION_OB": None,
            "Search": {},
            "datail_string": {
                "regnos": regnos,
            },
            "M_tran": {}
        }
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
    }
    r = requests.post(URL, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    buffer_rows = []           # เก็บข้อมูลชั่วคราวก่อนยิงขึ้นชีต
    last_flush = time.time()
    FLUSH_INTERVAL = 600       # 600 วินาที = 10 นาที

    # OPTIONAL: เขียน header ลงชีตถ้ายังไม่มี
    if len(ws_result.get_all_values()) == 0:
        header = [
            "notify_number",
            "regnos",
            "type",
            "lb_lct_type",
            "status_lct",
            "lb_status",
            "lb_appdate",
            "lb_expdate",
            "lb_trade_Tpop",
            "lb_trade_Tpop2",
            "lb_cosnm_Tpop",
            "lb_cosnm_Tpop2",
            "lb_application_name",
            "lb_applicability_name",
            "lb_mode",
            "lb_condition",
            "lb_usernm_pop",
            "lb_locat_pop",
            "physical_detail",
            "lb_NO_pop",
            "lb_fileattach_count",
        ]
        ws_result.append_row(header, value_input_option="RAW")

    total = len(notify_numbers)
    for idx, no in enumerate(notify_numbers, start=1):
        regnos = no.replace("-", "")

        try:
            res = get_detail_regnos(regnos)
        except Exception as e:
            print(f"[ERROR] {no}: {e}")
            # เก็บ log ว่า error ไว้ด้วย
            buffer_rows.append([no, regnos, "ERROR", str(e)] + [""] * 17)
        else:
            model = res.get("MODEL", res)
            detail = model.get("datail_string", {}) or {}

            row = [
                detail.get("lb_no_regnos"),
                detail.get("regnos"),
                detail.get("type"),
                detail.get("lb_lct_type"),
                detail.get("status_lct"),
                detail.get("lb_status"),
                detail.get("lb_appdate"),
                detail.get("lb_expdate"),
                detail.get("lb_trade_Tpop"),
                detail.get("lb_trade_Tpop2"),
                detail.get("lb_cosnm_Tpop"),
                detail.get("lb_cosnm_Tpop2"),
                detail.get("lb_application_name"),
                detail.get("lb_applicability_name"),
                detail.get("lb_mode"),
                detail.get("lb_condition"),
                detail.get("lb_usernm_pop"),
                detail.get("lb_locat_pop"),
                detail.get("physical_detail"),
                detail.get("lb_NO_pop"),
                detail.get("lb_fileattach_count"),
            ]
            buffer_rows.append(row)

        # log progress ทุก ๆ 500 rows
        if idx % 500 == 0:
            print(f"[PROGRESS] {idx}/{total} rows")

        # ถ้าเกิน 10 นาที หรือ buffer >= 200 แถว → flush ขึ้นชีต
        now = time.time()
        if (now - last_flush > FLUSH_INTERVAL) or len(buffer_rows) >= 200:
            ws_result.append_rows(buffer_rows, value_input_option="RAW")
            print(f"[FLUSH] wrote {len(buffer_rows)} rows at {datetime.now()}")
            buffer_rows.clear()
            last_flush = now

    # flush แถวที่เหลือ
    if buffer_rows:
        ws_result.append_rows(buffer_rows, value_input_option="RAW")
        print(f"[FLUSH] final {len(buffer_rows)} rows at {datetime.now()}")


if __name__ == "__main__":
    main()
