from datetime import datetime, timedelta

def get_date_str(days_from_today):
    """오늘 기준 특정 일수 후 날짜를 YYYYMMDD 형식으로 반환"""
    return (datetime.today() + timedelta(days=days_from_today)).strftime("%Y%m%d")

import requests
import logging
from config import KAKAO_REST_API_KEY

def address_to_coordinates(city, district, neighborhood, riname, lot_number):
    """
    법정동 주소를 WGS84 좌표(위도, 경도)로 변환 (카카오 API 사용)
    변환 실패 시 riname까지만 포함된 주소로 한 번 더 시도.
    """
    def request_coordinates(address):
        """카카오 API를 호출하여 주소 변환"""
        url = "https://dapi.kakao.com/v2/local/search/address.json"
        headers = {"Authorization": f"KakaoAK {KAKAO_REST_API_KEY}"}
        params = {"query": address}

        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            result = response.json()
            if result["documents"]:
                doc = result["documents"][0]
                return float(doc["y"]), float(doc["x"])  # (위도, 경도)
        return None, None  # 변환 실패 시

    # ✅ 전체 주소로 변환 시도
    address_parts = [part for part in [city, district, neighborhood, riname, lot_number] if part]
    full_address = " ".join(address_parts)
    logging.info(f"📍 변환 요청: {full_address}")

    lat, lon = request_coordinates(full_address)
    if lat is not None and lon is not None:
        logging.info(f"✅ 변환 성공: {full_address} → (위도: {lat}, 경도: {lon})")
        return lat, lon

    # ✅ 변환 실패 시, riname까지만 사용하여 재시도
    if riname:
        fallback_address = " ".join([city, district, neighborhood, riname])
        logging.warning(f"❌ 변환 실패, 재시도: {fallback_address}")

        lat, lon = request_coordinates(fallback_address)
        if lat is not None and lon is not None:
            logging.info(f"✅ 변환 성공 (재시도): {fallback_address} → (위도: {lat}, 경도: {lon})")
            return lat, lon

    logging.error(f"❌ 변환 실패: {full_address}")
    return None, None  # 최종적으로 변환 실패 시
