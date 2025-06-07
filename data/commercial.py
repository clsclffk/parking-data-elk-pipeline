import os
import requests
import json
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd

load_dotenv()
SERVICE_KEY = os.getenv("API_KEY")

df = pd.read_excel("data/서울시 주요 120장소 목록.xlsx")
area_list = df['AREA_NM'].dropna().unique().tolist()

results = []

for area in area_list:
    url = f"http://openapi.seoul.go.kr:8088/{SERVICE_KEY}/json/citydata/1/5/{area}"
    res = requests.get(url)
    if res.status_code != 200:
        print(f"상권 요청 실패: {area}")
        continue

    try:
        commercial_raw = res.json()["CITYDATA"].get("LIVE_CMRCL_STTS")
        if not commercial_raw:
            print(f"상권 정보 없음: {area}")
            continue
    except:
        print(f"JSON 파싱 오류: {area}")
        continue

    results.append({
        "timestamp": datetime.now().isoformat(),
        "area_name": area,
        "commercial": {
            "summary": {
                "activity_level": commercial_raw.get("AREA_CMRCL_LVL", ""),
                "payment_count": commercial_raw.get("AREA_SH_PAYMENT_CNT", ""),
                "min_amount": commercial_raw.get("AREA_SH_PAYMENT_AMT_MIN", ""),
                "max_amount": commercial_raw.get("AREA_SH_PAYMENT_AMT_MAX", "")
            },
            "categories": [
                {
                    "category": item.get("RSB_MID_CTGR", ""),
                    "level": item.get("RSB_PAYMENT_LVL", ""),
                    "payment_count": item.get("RSB_SH_PAYMENT_CNT", ""),
                    "amount_min": item.get("RSB_SH_PAYMENT_AMT_MIN", ""),
                    "amount_max": item.get("RSB_SH_PAYMENT_AMT_MAX", ""),
                    "stores": item.get("RSB_MCT_CNT", ""),
                    "timestamp": item.get("RSB_MCT_TIME", "")
                } for item in commercial_raw.get("CMRCL_RSB", [])
            ]
        }
    })

os.makedirs("data", exist_ok=True)
with open("data/commercial_data.jsonl", "w", encoding="utf-8") as f:
    for r in results:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")

print("상권 데이터 저장 완료")
