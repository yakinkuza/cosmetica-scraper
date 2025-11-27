import requests
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os
import time

URL = "https://cosmetica.fda.moph.go.th/CMT_SEARCH_BACK_NEW/Home/FUNCTION_CENTER"

INPUT_EXCEL = "ข้อมูลผู้ประกอบการ.xlsx"   # ชื่อไฟล์ input
INPUT_NOTIFY_COL = "เลขที่จดแจ้ง"          # ชื่อคอลัมน์เลขจดแจ้งในไฟล์
OUTPUT_EXCEL = "cosmetica_results.xlsx"     # ไฟล์ผลลัพธ์

def get_detail_regnos(regnos: str, session: requests.Session, retries: int = 3, backoff: float = 1.0):
    """
    เรียก API ทีละ regnos + มี retry
    """
    for attempt in range(1, retries + 1):
        try:
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

            r = session.post(URL, headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/plain, */*",
            }, data=json.dumps(payload), timeout=30)

            r.raise_for_status()
            data = r.json()
            model = data.get("MODEL", data)
            detail = model.get("datail_string") or {}

            # คืนทุก field ที่มีใน datail_string
            row = dict(detail)
            row["__success"] = True
            row["__error"] = ""
            return row

        except Exception as e:
            if attempt == retries:
                # เก็บ error ไว้ตรวจทีหลัง
                return {
                    "regnos": regnos,
                    "__success": False,
                    "__error": str(e),
                }
            # ถ้า error ให้หน่วงเวลาแล้วลองใหม่ (backoff)
            time.sleep(backoff * attempt)

def main():
    # ====== 1. อ่านเลขจดแจ้งจาก Excel ======
    src = pd.read_excel(INPUT_EXCEL)
    notify_raw = src[INPUT_NOTIFY_COL].dropna().astype(str).str.strip()

    # แปลง "10-2-6700038284" → "1026700038284"
    regnos_list = notify_raw.str.replace("-", "", regex=False)

    # ทำตาราง mapping ไว้
    mapping_df = pd.DataFrame({
        "notify_number": notify_raw,
        "regnos": regnos_list,
    })

    # ====== 2. โหลดผลเก่ามาดูว่าทำไปแล้วตัวไหนบ้าง (resume) ======
    existing_df = None
    done_regnos = set()

    if os.path.exists(OUTPUT_EXCEL):
        existing_df = pd.read_excel(OUTPUT_EXCEL, dtype=str)
        if "regnos" in existing_df.columns:
            done_regnos = set(existing_df["regnos"].dropna().astype(str))

    # เลือกเฉพาะ regnos ที่ยังไม่เคยดึง
    todo = mapping_df[~mapping_df["regnos"].isin(done_regnos)].reset_index(drop=True)

    if todo.empty:
        print("ทุก regnos ถูกดึงข้อมูลแล้ว ไม่มีงานค้างค่ะ")
        return

    print(f"จำนวน regnos ที่ต้องดึงเพิ่ม: {len(todo)}")

    results = []

    # ====== 3. ยิง API แบบขนาน + progress bar ======
    max_workers = 30  # ปรับได้ตามใจ
    session = requests.Session()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(get_detail_regnos, row.regnos, session): row
            for _, row in todo.iterrows()
        }

        with tqdm(total=len(futures), desc="Fetching", unit="item") as pbar:
            for future in as_completed(futures):
                src_row = futures[future]
                try:
                    detail_row = future.result()
                except Exception as e:
                    detail_row = {
                        "regnos": src_row.regnos,
                        "__success": False,
                        "__error": f"unhandled: {e}",
                    }

                # ผูก notify_number กลับเข้าไป
                detail_row.setdefault("regnos", src_row.regnos)
                detail_row.setdefault("notify_number", src_row.notify_number)

                results.append(detail_row)
                pbar.update(1)

    # ====== 4. รวมกับผลเก่า + บันทึกลง Excel ======
    new_df = pd.DataFrame(results)

    # ถ้ามีผลเก่า ให้ concat และ drop duplicates
    if existing_df is not None:
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
        # กัน regnos ซ้ำ
        if "regnos" in final_df.columns:
            final_df = final_df.drop_duplicates(subset=["regnos"], keep="last")
    else:
        final_df = new_df

    final_df.to_excel(OUTPUT_EXCEL, index=False)
    print(f"บันทึกผลแล้วที่ {OUTPUT_EXCEL}")

if __name__ == "__main__":
    main()
