import os
import pandas as pd
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from utils import (
    fetch_commercial_data,
    add_search_keyword,
    add_geolocation_from_kakao,
    add_parking_count
)
from geopy.distance import geodesic
from datetime import datetime

load_dotenv()

def upload_to_elasticsearch(df, index_name):
    es = Elasticsearch("http://localhost:9200")

    # 인덱스가 없으면 생성
    if not es.indices.exists(index=index_name):
        mapping = {
            "mappings": {
                "properties": {}
            }
        }
        if "location" in df.columns:
            mapping["mappings"]["properties"]["location"] = {"type": "geo_point"}

        es.indices.create(index=index_name, body=mapping)
        print(f"인덱스 '{index_name}' 생성 완료")
    actions = []

    for _, row in df.iterrows():
        source = row.dropna().to_dict()

        # location 필드가 있는 경우만 업로드
        if "location" in df.columns and not row.get("location"):
            continue

        # 문서 고유 ID 생성 (예: 상권명_수집시간)
        _id = f"{row.get('search_keyword')}_{row.get('timestamp')}"

        actions.append({
            "_index": index_name,
            "_id": _id,              # <- 여기에 ID를 명시해야 덮어쓰기가 가능
            "_source": source
        })

    if actions:
        helpers.bulk(es, actions)
        print(f"[{index_name}] Elasticsearch 업로드 완료: {len(actions)}건")
    else:
        print(f"[{index_name}] 업로드할 유효한 데이터가 없습니다.")

def get_parking_data_from_elasticsearch():
    """
    Elasticsearch에서 주차장 데이터를 가져오기 위한 함수
    """
    es = Elasticsearch("http://localhost:9200")
    query = {
        "query": {
            "match_all": {}
        }
    }
    
    # Elasticsearch에서 'seoul_parking' 인덱스를 조회하여 주차장 데이터 가져오기
    response = es.search(index="seoul_parking", body=query, size=10000)  # size는 필요한 데이터의 크기 조절
    
    # Elasticsearch에서 반환된 데이터를 DataFrame 형식으로 변환
    parking_data = [hit["_source"] for hit in response["hits"]["hits"]]
    parking_df = pd.DataFrame(parking_data)
    
    # location을 geo_point로 변환
    parking_df["location"] = parking_df.apply(
    lambda row: {
        "lat": float(row["latitude"]), 
        "lon": float(row["longitude"])
    } if pd.notnull(row.get("latitude")) and pd.notnull(row.get("longitude"))
    else None,
    axis=1
)
    
    return parking_df

def add_avg_available_rate(summary_df, parking_df):
    avg_rates = []

    for _, row in summary_df.iterrows():
        center = (row["latitude"], row["longitude"])

        # 반경 300m 내 주차장 필터링
        nearby = parking_df[
            parking_df["location"].apply(
                lambda loc: geodesic(center, (loc["lat"], loc["lon"])).meters <= 300
            )
        ]

        # 평균 가용률 계산
        if not nearby.empty:
            avg_rate = nearby["available_rate"].dropna().mean()
        else:
            avg_rate = None

        avg_rates.append(avg_rate)

    summary_df["avg_available_rate_300m"] = avg_rates
    return summary_df

def main():
    print("서울시 상권 데이터 수집 및 업로드 시작")

    # 1. 원본 데이터 수집
    summary_df, categories_df = fetch_commercial_data()

    # 2. search_keyword 열 추가   
    summary_df = add_search_keyword(summary_df)
    categories_df = add_search_keyword(categories_df)

    # 3. 위도/경도 및 geo_point 열 추가
    summary_df = add_geolocation_from_kakao(summary_df)

    # 4. 주차장 데이터 Elasticsearch에서 불러오기
    parking_df = get_parking_data_from_elasticsearch()

    # 5. 주차장 반경 300m 개수 추가
    summary_df = add_parking_count(summary_df, parking_df)

    # 6. 데이터 수집 시각 컬럼 추가
    now_ts = datetime.now().isoformat()
    summary_df["timestamp"] = now_ts
    categories_df["timestamp"] = now_ts

    # 7. 평균 주차장 가용률 추가
    summary_df = add_avg_available_rate(summary_df, parking_df)

    # 8. 수치형으로 변환
    summary_df["payment_count"] = pd.to_numeric(summary_df["payment_count"], errors="coerce")
    categories_df["payment_count"] = pd.to_numeric(categories_df["payment_count"], errors="coerce")

    # 9. Elasticsearch 업로드
    upload_to_elasticsearch(summary_df, index_name="seoul_commercial")
    upload_to_elasticsearch(categories_df, index_name="seoul_commercial_categories")

if __name__ == "__main__":
    main()
