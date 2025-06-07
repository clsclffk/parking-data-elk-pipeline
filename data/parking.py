import requests
import os
import json
from dotenv import load_dotenv
import pandas as pd

# 환경 변수 로드 (.env에 API_KEY=발급받은키 형태로 저장돼 있어야 함)
load_dotenv()
API_KEY = os.getenv("API_KEY")

# 기본값 설정
BASE_URL = "http://openapi.seoul.go.kr:8088"
SERVICE = "GetParkingInfo"
DATA_TYPE = "json"
BATCH_SIZE = 1000  # 한 번에 요청할 데이터 개수

# 1. 총 데이터 개수 확인
first_url = f"{BASE_URL}/{API_KEY}/{DATA_TYPE}/{SERVICE}/1/1"
response = requests.get(first_url).json()
total_count = response["GetParkingInfo"]["list_total_count"]
print(f"총 주차장 수: {total_count}")

# 2. 전체 데이터 반복 수집
all_rows = []

for start in range(1, total_count + 1, BATCH_SIZE):
    end = min(start + BATCH_SIZE - 1, total_count)
    url = f"{BASE_URL}/{API_KEY}/{DATA_TYPE}/{SERVICE}/{start}/{end}"

    res = requests.get(url)
    if res.status_code == 200:
        try:
            rows = res.json()["GetParkingInfo"]["row"]
            all_rows.extend(rows)
            print(f"수집 완료: {start} ~ {end}")
        except Exception as e:
            print(f"데이터 파싱 실패: {start} ~ {end} / {e}")
    else:
        print(f"요청 실패: {start} ~ {end} / status {res.status_code}")

# 3. CSV 저장
df = pd.DataFrame(all_rows)
os.makedirs("data", exist_ok=True)
df.to_csv("data/seoul_public_parking.csv", index=False, encoding="utf-8-sig")
print("저장 완료: data/seoul_public_parking.csv")
