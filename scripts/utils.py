import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import holidays
from geopy.distance import geodesic
from elasticsearch import Elasticsearch, helpers

load_dotenv()
API_KEY = os.getenv("API_KEY")
KAKAO_API_KEY = os.getenv("KAKAO_API_KEY")
headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}

kr_holidays = holidays.KR()  # 한국 공휴일


## 1. 주차장 데이터 
# 1-1. 데이터 불러오기
def fetch_parking_data():
    BASE_URL = "http://openapi.seoul.go.kr:8088"
    SERVICE = "GetParkingInfo"
    DATA_TYPE = "json"
    BATCH_SIZE = 1000

    # 총 데이터 개수 확인
    first_url = f"{BASE_URL}/{API_KEY}/{DATA_TYPE}/{SERVICE}/1/1"
    response = requests.get(first_url).json()
    total_count = response["GetParkingInfo"]["list_total_count"]

    # 전체 데이터 반복 수집
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

    # 데이터프레임 반환
    df = pd.DataFrame(all_rows)
    return df

# 1-2. 노상 & 실시간 데이터 제공 & 가용공간이 음수가 아닌 데이터 필터링 & 실시간 현황이 업데이트 되지 않는 데이터 제거
def filter_valid_parking(df):
    df = df.copy()

    # 숫자형 변환
    df["TPKCT"] = pd.to_numeric(df["TPKCT"], errors="coerce")
    df["NOW_PRK_VHCL_CNT"] = pd.to_numeric(df["NOW_PRK_VHCL_CNT"], errors="coerce")

    # 날짜 파싱
    df["NOW_PRK_VHCL_UPDT_TM"] = pd.to_datetime(df["NOW_PRK_VHCL_UPDT_TM"], errors="coerce")
    today = datetime.now().date()
    df["update_date"] = df["NOW_PRK_VHCL_UPDT_TM"].dt.date

    # 필터링 조건
    filtered = df[
        (df["PKLT_TYPE"] == "NW") &                            # 노상
        (df["PRK_STTS_YN"] == "1") &                           # 실시간 제공
        (df["TPKCT"].notnull()) &
        (df["NOW_PRK_VHCL_CNT"].notnull()) &
        ((df["TPKCT"] - df["NOW_PRK_VHCL_CNT"]) >= 0) &        # 가용 공간 >= 0
        (df["update_date"] == today)                           # 오늘 업데이트된 데이터만
    ].copy()

    return filtered

# 1-3. 위도, 경도 열 만들고 좌표 열 만들기
def add_geolocation(df):
    """
    - 주소(ADDR)를 기준으로 위도(latitude), 경도(longitude) 컬럼 생성
    - location 컬럼: Elasticsearch의 geo_point 형태 ({ "lat": 위도, "lon": 경도 })
    """
    from time import sleep

    df = df.copy()
    
    # 좌표 변환 함수
    def geocode(address):
        url = "https://dapi.kakao.com/v2/local/search/address.json"
        params = {"query": address}
        res = requests.get(url, headers=headers, params=params)
        if res.status_code == 200:
            result = res.json()
            if result['documents']:
                return result['documents'][0]['y'], result['documents'][0]['x']
        return None, None

    # 위도/경도 생성
    df["latitude"], df["longitude"] = zip(*df["ADDR"].apply(geocode))

    # location 필드 생성 (geo_point용)
    df["location"] = df.apply(
        lambda row: {"lat": float(row["latitude"]), "lon": float(row["longitude"])}
        if pd.notnull(row["latitude"]) and pd.notnull(row["longitude"])
        else None,
        axis=1
    )

    return df

# 1-4. 가용 공간 열 및 현재 운영 여부 열 만들기
def compute_availability_and_status(df): 
    """
    주차장 데이터에서 가용률(available_rate)과 현재 운영 여부(is_operating_now)를 계산

    1. available_rate:
    - 가용률 = (총 주차 가능 면수 - 현재 주차 차량 수) / 총 주차 가능 면수
    - 예: 총 100면 중 70대가 주차 중이면 → (100 - 70) / 100 = 0.30 (30%)

    2. is_operating_now:
    - 현재 시간이 운영 시간 내에 있는지를 기준으로 '운영 중' 또는 '운영 종료'로 표시
    - 운영 시간은 요일(평일, 주말, 공휴일)에 따라 다르게 적용됨
    - 시간 포맷은 'HHMM' (예: 0830, 2130)
    """
    df = df.copy()

    # available_rate 계산
    df["available_rate"] = (
        (df["TPKCT"] - df["NOW_PRK_VHCL_CNT"]) / df["TPKCT"]
    ).round(2)

    def get_operating_status(row):
        now = datetime.now()
        now_time = int(now.strftime("%H%M"))
        today = now.date()

        try:
            # 공휴일 여부 판단
            if today in kr_holidays:
                start = int(row["LHLDY_OPER_BGNG_TM"])
                end = int(row["LHLDY_OPER_END_TM"])
            else:
                weekday = now.weekday()
                if weekday < 5:
                    start = int(row["WD_OPER_BGNG_TM"])
                    end = int(row["WD_OPER_END_TM"])
                elif weekday == 5:
                    start = int(row["WE_OPER_BGNG_TM"])
                    end = int(row["WE_OPER_END_TM"])
                else:
                    start = int(row["LHLDY_OPER_BGNG_TM"])
                    end = int(row["LHLDY_OPER_END_TM"])
        except:
            return "운영 종료"

        if start <= now_time <= end:
            return "운영 중"
        else:
            return "운영 종료"

    # 적용
    df["is_operating_now"] = df.apply(get_operating_status, axis=1)

    return df


## 2. 상권 데이터 
# 2-1. 상권 데이터 불러오기
def fetch_commercial_data(excel_path: str = None):
    """
    서울시 주요 120개 장소의 상권 실시간 데이터를 API에서 불러와
    summary_df와 categories_df로 반환

    Parameters:
        excel_path (str): 상권 이름 목록이 담긴 엑셀 경로

    Returns:
        summary_df (pd.DataFrame): 상권 요약 정보
        categories_df (pd.DataFrame): 업종별 상세 정보
    """

    if excel_path is None:
        # 이 파일(utils.py)의 상위 디렉토리에 있는 data 폴더를 기준으로 경로 설정
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        excel_path = os.path.join(base_dir, "data", "서울시 주요 120장소 목록.xlsx")
        
    df = pd.read_excel(excel_path)
    area_list = df['AREA_NM'].dropna().unique().tolist()

    summary_rows = []
    category_rows = []

    for area in area_list:
        url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/json/citydata/1/5/{area}"
        res = requests.get(url)
        if res.status_code != 200:
            print(f"[요청 실패] {area}")
            continue

        try:
            commercial_raw = res.json()["CITYDATA"].get("LIVE_CMRCL_STTS")
            if not commercial_raw:
                print(f"[상권 없음] {area}")
                continue
        except Exception as e:
            print(f"[파싱 오류] {area}: {e}")
            continue

        timestamp = datetime.now().isoformat()

        # 요약 정보
        summary = commercial_raw.get("AREA_CMRCL_LVL", "")
        summary_rows.append({
            "timestamp": timestamp,
            "area_name": area,
            "activity_level": commercial_raw.get("AREA_CMRCL_LVL", ""),
            "payment_count": commercial_raw.get("AREA_SH_PAYMENT_CNT", ""),
            "min_amount": commercial_raw.get("AREA_SH_PAYMENT_AMT_MIN", ""),
            "max_amount": commercial_raw.get("AREA_SH_PAYMENT_AMT_MAX", "")
        })

        # 업종별 상세 정보
        for item in commercial_raw.get("CMRCL_RSB", []):
            category_rows.append({
                "timestamp": item.get("RSB_MCT_TIME", ""),
                "area_name": area,
                "category": item.get("RSB_MID_CTGR", ""),
                "level": item.get("RSB_PAYMENT_LVL", ""),
                "payment_count": item.get("RSB_SH_PAYMENT_CNT", ""),
                "amount_min": item.get("RSB_SH_PAYMENT_AMT_MIN", ""),
                "amount_max": item.get("RSB_SH_PAYMENT_AMT_MAX", ""),
                "stores": item.get("RSB_MCT_CNT", "")
            })

    summary_df = pd.DataFrame(summary_rows)
    categories_df = pd.DataFrame(category_rows)

    return summary_df, categories_df

# 2-1. search_keyword열 만들기

mapping_dict = {
    '강남 MICE 관광특구': '코엑스',
    '동대문 관광특구': '현대시티아울렛 동대문점', 
    '명동 관광특구': '명동성당',
    '이태원 관광특구': '해밀톤호텔', 
    '잠실 관광특구': '석촌호수', 
    '종로·청계 관광특구': '종각역',
    '홍대 관광특구': '홍대입구역',
    '광화문·덕수궁': '광화문',
    '보신각': '보신각',
    '가산디지털단지역': '가산디지털단지역',
    '강남역': '강남역',
    '건대입구역': '건대입구역',
    '고덕역': '고덕역',
    '고속터미널역': '고속터미널역',
    '교대역': '교대역',
    '구로디지털단지역': '구로디지털단지역',
    '구로역': '구로역',
    '군자역': '군자역',
    '대림역': '대림역',
    '동대문역': '동대문역',
    '뚝섬역': '뚝섬역',
    '미아사거리역': '미아사거리역',
    '발산역': '발산역',
    '사당역': '사당역',
    '서울대입구역': '서울대입구역',
    '서울식물원·마곡나루역': '서울식물원',
    '서울역': '서울역',
    '선릉역': '선릉역',
    '성신여대입구역': '성신여대입구역',
    '수유역': '수유역',
    '신논현역·논현역': '신논현역',
    '신도림역': '신도림역',
    '신림역': '신림역',
    '신촌·이대역': '신촌역',
    '양재역': '양재역',
    '역삼역': '역삼역',
    '연신내역': '연신내역',
    '오목교역·목동운동장': '오목교역',
    '왕십리역': '왕십리역',
    '용산역': '용산역',
    '이태원역': '이태원역',
    '장지역': '장지역',
    '장한평역': '장한평역',
    '천호역': '천호역',
    '총신대입구(이수)역': '총신대입구역',
    '충정로역': '충정로역',
    '합정역': '합정역',
    '혜화역': '혜화역',
    '홍대입구역(2호선)': '홍대입구역 2호선',
    '회기역': '회기역',
    '가락시장': '가락시장',
    '가로수길': '가로수길',
    '광장(전통)시장': '광장시장',
    '김포공항': '김포공항',
    '노량진': '노량진동',
    '덕수궁길·정동길': '덕수궁길',
    '북촌한옥마을': '북촌한옥마을',
    '서촌': '서촌한옥마을',
    '성수카페거리': '성수동카페거리',
    '쌍문역': '쌍문역',
    '압구정로데오거리': '압구정로데오거리',
    '여의도': '여의도',
    '연남동': '연남동',
    '영등포 타임스퀘어': '타임스퀘어',
    '용리단길': '용리단길',
    '이태원 앤틱가구거리': '이태원앤틱가구거리',
    '인사동': '인사동',
    '창동 신경제 중심지': '창동역',
    '청담동 명품거리': '청담동명품거리',
    '청량리 제기동 일대 전통시장': '청량리전통시장',
    '해방촌·경리단길': '경리단길',
    'DDP(동대문디자인플라자)': '동대문디자인플라자',
    'DMC(디지털미디어시티)': '디지털미디어시티역',
    '북창동 먹자골목': '북창동먹자골목',
    '남대문시장': '남대문시장',
    '익선동': '서울 종로구 익선동',
    '신정네거리역': '신정네거리역',
    '잠실새내역': '잠실새내역',
    '잠실역': '잠실역',
    '잠실롯데타워 일대': '롯데월드타워',
    '송리단길·호수단길': '송리단길',
    '신촌 스타광장': '스타광장'
}

def add_search_keyword(df):
    """
    search_keyword 열 추가 (상권명 → 실제 검색어로 매핑)

    Parameters:
        df (pd.DataFrame): summary_df or categories_df
        mapping_dict (dict): {'원래 상권명': '검색용 키워드'}

    Returns:
        pd.DataFrame
    """
    df = df.copy()
    df["search_keyword"] = df["area_name"].map(mapping_dict)
    return df


# 2-2. 위도, 경도 열 만들고 좌표 열 만들기
def add_geolocation_from_kakao(df):
    def geocode(keyword):
        url = "https://dapi.kakao.com/v2/local/search/keyword.json"
        params = {"query": keyword}
        res = requests.get(url, headers=headers, params=params)
        if res.status_code == 200:
            result = res.json()
            if result['documents']:
                lat = float(result['documents'][0]['y'])
                lon = float(result['documents'][0]['x'])
                return lat, lon
        return None, None

    df = df.copy()
    df["latitude"], df["longitude"] = zip(*df["search_keyword"].apply(geocode))

    df["location"] = df.apply(
        lambda row: {"lat": row["latitude"], "lon": row["longitude"]}
        if pd.notnull(row["latitude"]) and pd.notnull(row["longitude"])
        else None,
        axis=1
    )

    return df

# 2-3. 300m 반경 내 주차장 개수 열 만들기
def add_parking_count(summary_df, parking_df, radius_m=300):
    """
    반경 radius_m(m 단위) 내 주차장 개수 카운트
    
    Parameters:
        summary_df (pd.DataFrame): 상권 데이터 (location 포함)
        parking_df (pd.DataFrame): 주차장 데이터 (location 포함)
        radius_m (int): 반경 거리 (기본: 300m)

    Returns:
        pd.DataFrame: parking_count_300m 열 추가됨
    """
    summary_df = summary_df.copy()
    counts = []

    for _, area in summary_df.iterrows():
        area_loc = area["location"]
        if not area_loc:
            counts.append(0)
            continue
        count = 0
        for _, park in parking_df.iterrows():
            park_loc = park["location"]
            if not park_loc:
                continue
            distance = geodesic(
                (area_loc["lat"], area_loc["lon"]),
                (park_loc["lat"], park_loc["lon"])
            ).meters
            if distance <= radius_m:
                count += 1
        counts.append(count)

    summary_df["parking_count_300m"] = counts
    return summary_df





