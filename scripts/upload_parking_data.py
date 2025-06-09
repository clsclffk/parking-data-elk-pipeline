import os
import pandas as pd
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from utils import (
    fetch_parking_data,
    filter_valid_parking,
    add_geolocation,
    compute_availability_and_status,
)
from datetime import datetime 
import pytz

load_dotenv()

def upload_to_elasticsearch(df, index_name="seoul_parking"):
    """
    주어진 DataFrame을 Elasticsearch 인덱스로 bulk 업로드
    """
    es = Elasticsearch("http://localhost:9200")

    # 인덱스가 없다면 생성하면서 location 필드를 geo_point로 지정
    if not es.indices.exists(index=index_name):
        es.indices.create(
            index=index_name,
            body={
                "mappings": {
                    "properties": {
                        "location": {"type": "geo_point"},
                        "timestamp": {"type": "date"}
                    }
                }
            }
        )
        print(f"인덱스 '{index_name}' 생성 및 geo_point 매핑 설정 완료")

    actions = [
    {
        "_index": index_name,
        "_id": f"{row.get('PKLT_NM')}_{row.get('timestamp')}",
        "_source": {
            "parking_name": row.get("PKLT_NM"),                       # 주차장명
            "address": row.get("ADDR"),                               # 주소
            "latitude": row.get("latitude"),                          # 위도
            "longitude": row.get("longitude"),                        # 경도
            "location": row.get("location"),                          # geo_point
            "available_rate": row.get("available_rate"),              # 가용률 = (전체 - 현재 차량 수) / 전체
            "is_operating_now": row.get("is_operating_now"),          # 현재 운영 여부 (운영 중 / 운영 종료)
            "update_time": row.get("NOW_PRK_VHCL_UPDT_TM"),           # 실시간 정보 업데이트 시각
            "is_paid": row.get("PAY_YN_NM"),                          # 유료 여부 (유료 / 무료)
            "saturday_free": row.get("SAT_CHGD_FREE_NM"),             # 토요일 무료 여부
            "holiday_free": row.get("LHLDY_CHGD_FREE_SE_NAME"),       # 공휴일 무료 여부
            "basic_charge": row.get("BSC_PRK_CRG"),                   # 기본 요금 (원)
            "basic_time": row.get("BSC_PRK_HR"),                      # 기본 시간 (분)
            "add_charge": row.get("ADD_PRK_CRG"),                     # 추가 요금 (원)
            "add_time": row.get("ADD_PRK_HR"),                        # 추가 시간 (분)
            "hourly_rate": row.get("hourly_rate"),                    # 시간당 요금 (원/시간) = (기본 요금 / 기본 시간) * 60
            "timestamp": row.get("timestamp"),                        # 수집 시각 (스크립트 실행 시점)
            "available_status": row.get("available_status"),          # 혼잡도 상태 (여유 / 보통 / 혼잡 / 정보 없음)
            "district": row.get("district"),                          # 구별 주소 (예: "강남구")
            "weekday": row.get("weekday"),                            # 요일 (예: "월")
            "weekday_order": row.get("weekday_order"),                # 요일 정렬용 인덱스 (0~6)
        }
    }
    for _, row in df.iterrows()
    if row.get("location")
]

    if actions:
        helpers.bulk(es, actions)
        print(f"Elasticsearch 업로드 완료: {len(actions)}건")
    else:
        print("업로드할 유효한 데이터가 없습니다.")

def main():
    print("서울시 주차장 데이터 수집 및 업로드 시작")

    # 1. 원본 데이터 수집
    df_raw = fetch_parking_data()

    # 2. 유효 데이터 필터링
    df_valid = filter_valid_parking(df_raw)

    # 3. 위도/경도 및 geo_point 생성
    df_geo = add_geolocation(df_valid)

    # 4. 가용률 + 운영 여부 추가
    df_status = compute_availability_and_status(df_geo)

    # 5. timestamp 열 추가
    tz = pytz.timezone("Asia/Seoul")
    now_kst = datetime.now(tz)
    df_status["timestamp"] = [now_kst] * len(df_status)

    # 5-1. 요일 파생 컬럼 추가 (예: "월", "화", ..., "일")
    df_status["weekday"] = df_status["timestamp"].dt.dayofweek.map({
        0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"
    })

    # 5-2. 요일 정렬용 컬럼 (요일 순서대로 시각화용 정렬 지원)
    df_status["weekday_order"] = df_status["timestamp"].dt.dayofweek

    # 6. 시간당 요금 계산
    def calculate_hourly_rate(row):
        try:
            if pd.notnull(row["BSC_PRK_CRG"]) and pd.notnull(row["BSC_PRK_HR"]) and row["BSC_PRK_HR"] > 0:
                return round((row["BSC_PRK_CRG"] / row["BSC_PRK_HR"]) * 60)
        except Exception:
            return None
        return None

    df_status["hourly_rate"] = df_status.apply(calculate_hourly_rate, axis=1)


    # 7. 혼잡도 상태 구분 컬럼 추가
    def classify_available_rate(rate):
        if pd.isnull(rate):
            return "정보 없음"
        elif rate < 0.3:
            return "혼잡"
        elif rate < 0.7:
            return "보통"
        else:
            return "여유"

    df_status["available_status"] = df_status["available_rate"].apply(classify_available_rate)

    # 8. 구별 주소 추출
    def extract_district(address):
        try:
            return [word for word in address.split() if word.endswith("구")][0]
        except:
            return None

    df_status["district"] = df_status["ADDR"].apply(extract_district)

    # 9. Elasticsearch 업로드
    upload_to_elasticsearch(df_status)

if __name__ == "__main__":
    main()
