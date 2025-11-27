import requests
import json
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os

# ================== CONFIG ==================

# 1) URL ของระบบ อย. เครื่องสำอาง
URL = "https://cosmetica.fda.moph.go.th/CMT_SEARCH_BACK_NEW/Home/FUNCTION_CENTER"

# 2) Google Sheets (เปลี่ยน gid ถ้าใช้ชีทอื่น)
#    ต้องเปิดสิทธิ์เป็น Anyone with the link -> Viewer
SHEET_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/19ciuRoIOKVe3Rdrzi7HBAw_Sq_bEebwu/"
    "export?format=csv&gid=1412556234"
)

# 3) ชื่อไฟล์ผลลัพธ์
OUTPUT_EXCEL = "fda_results.xlsx"
ERROR_EXCEL = "fda_errors.xlsx"

# 4) ตั้งจำนวน concurrent workers (เยอะ = ไว แต่ระวัง server ล้ม)
MAX_WORKERS = 8

# 5) จำนวน retry ต่อเลข + ดีเลย์เริ่มต้น
MAX_RETRIES = 3
BASE_DELAY = 2  # วินาที

# 6) ชื่อคอลัมน์ใน Google Sheets ที่เก็บเลขจดแจ้ง
#    👉 แก้ให้ตรงกับชื่อในไฟล์ของคุณบีม เช่น "เลขที่จดแจ้ง" หรือ "notify_number"
COL_NOTIFY_NUMBER = "เลขที่จดแจ้ง"

# ================== ฟังก์ชันช่วย ==================


def get_detail_regnos(regnos: str, session: requests.Session) -> dict:
    """
    เรียก API get_detail_regnos โดยส่ง regnos (ตัวเลขล้วน) แล้วคืนค่า JSON ทั้งก้อน
    มี retry ในตัว
    """
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
            "M_tran": {},
        }
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(URL, headers=headers, data=json.dumps(payload), timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            # backoff
            time.sleep(BASE_DELAY * attempt)


def flatten_detail(detail: dict, notify_number: str, clean_regnos: str) -> dict:
    """
    แปลง datail_string ให้กลายเป็น 1 แถว (dict)
    - ถ้า value เป็น dict/list -> แปลงเป็น JSON string
    - เติม notify_number และ regnos ให้แน่ใจว่ามี
    """
    row = {}
    for k, v in (detail or {}).items():
        if isinstance(v, (dict, list)):
            row[k] = json.dumps(v, ensure_ascii=False)
        else:
            row[k] = v

    # เติมฟิลด์เพิ่มไว้ใช้เช็คซ้ำ/อ้างอิง
    row.setdefault("notify_number", notify_number)
    row.setdefault("regnos", clean_regnos)

    return row


def load_notify_numbers() -> list[str]:
    """
    โหลดเลขจดแจ้งทั้งหมดจาก Google Sheets (ผ่าน CSV export)
    """
    df = pd.read_csv(SHEET_CSV_URL, dtype=str)
    if COL_NOTIFY_NUMBER not in df.columns:
        raise ValueError(
            f"ไม่พบคอลัมน์ '{COL_NOTIFY_NUMBER}' ใน Google Sheets "
            f"กรุณาเปลี่ยนชื่อ COL_NOTIFY_NUMBER ให้ตรงในสคริปต์"
        )
    nums = (
        df[COL_NOTIFY_NUMBER]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .tolist()
    )
    # ลบ duplicate
    nums = list(dict.fromkeys(nums))
    return nums


def load_existing_results():
    """
    ถ้ามีไฟล์ผลลัพธ์เก่า -> โหลดมาเพื่อใช้ skip เลขที่ทำไปแล้ว
    """
    if not os.path.exists(OUTPUT_EXCEL):
        return None, set()

    df = pd.read_excel(OUTPUT_EXCEL, dtype=str)
    done = set(df.get("notify_number", []).dropna().tolist())
    return df, done


def worker_task(notify_number: str, session: requests.Session) -> dict:
    """
    ฟังก์ชันที่ใช้ใน ThreadPool
    คืนค่า: dict ของข้อมูล detail (flatten แล้ว) หรือ dict ที่มี error
    """
    # แปลง "10-2-6700038284" -> "1026700038284"
    clean_regnos = notify_number.replace("-", "")

    try:
        res = get_detail_regnos(clean_regnos, session)
        model = res.get("MODEL", res)
        detail = model.get("datail_string") or {}

        if not detail:
            return {
                "notify_number": notify_number,
                "regnos": clean_regnos,
                "error": "EMPTY_DETAIL",
            }

        row = flatten_detail(detail, notify_number, clean_regnos)
        return row

    except Exception as e:
        return {
            "notify_number": notify_number,
            "regnos": clean_regnos,
            "error": str(e),
        }


def main():
    # 1) โหลดเลขจดแจ้ง
    print("โหลดเลขจดแจ้งจาก Google Sheets...")
    notify_numbers = load_notify_numbers()
    print(f"พบเลขจดแจ้งทั้งหมด: {len(notify_numbers):,} รายการ")

    # 2) เช็คว่ามีผลลัพธ์เก่าไหม (ใช้ resume ได้)
    existing_df, done_set = load_existing_results()
    if done_set:
        print(f"พบผลลัพธ์เก่าแล้ว {len(done_set):,} รายการ จะข้ามเลขที่ทำแล้วให้")
        to_do = [n for n in notify_numbers if n not in done_set]
    else:
        to_do = notify_numbers

    print(f"ต้องดึงข้อมูลเพิ่ม: {len(to_do):,} รายการ")

    results = []
    errors = []

    if not to_do:
        print("ไม่มีเลขใหม่ให้ดึง ออกจากโปรแกรม")
        return

    # 3) ยิง concurrent requests + progress bar
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_map = {
                executor.submit(worker_task, n, session): n for n in to_do
            }

            with tqdm(
                total=len(future_map),
                desc="Scraping FDA",
                unit="product",
                ncols=100,
            ) as pbar:
                for fut in as_completed(future_map):
                    row = fut.result()
                    if "error" in row:
                        errors.append(row)
                    else:
                        results.append(row)
                    pbar.update(1)

    # 4) รวมผลลัพธ์ + เซฟไฟล์
    if results:
        df_new = pd.DataFrame(results)
        if existing_df is not None:
            df_all = pd.concat([existing_df, df_new], ignore_index=True)
        else:
            df_all = df_new
        df_all.to_excel(OUTPUT_EXCEL, index=False)
        print(f"✅ บันทึกผลทั้งหมดที่ '{OUTPUT_EXCEL}' แล้ว ({len(df_all):,} แถว)")
    else:
        print("⚠ ไม่มีผลลัพธ์ใหม่ (results ว่าง)")

    if errors:
        df_err = pd.DataFrame(errors)
        df_err.to_excel(ERROR_EXCEL, index=False)
        print(
            f"⚠ มี error {len(errors):,} รายการ "
            f"บันทึกไว้ที่ '{ERROR_EXCEL}' แล้ว"
        )
    else:
        print("✅ ไม่มี error จากการดึงข้อมูล")


if __name__ == "__main__":
    main()
