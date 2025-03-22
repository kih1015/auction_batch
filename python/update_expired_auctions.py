import logging
import time
from datetime import datetime
import requests
from pymongo import MongoClient

from config import MONGO_URI, DB_NAME, COLLECTION_NAME, HEADERS

# MongoDB 연결 설정
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
auctions_collection = db[COLLECTION_NAME]

# API URL 설정
AUCTION_HISTORY_URL = "https://www.courtauction.go.kr/pgj/pgj15A/selectCsDtlDxdyDts.on"

# 기일 종류 코드 매핑
AUCTION_KIND_MAPPING = {
    "매각기일": "01",
    "매각결정기일": "02",
    "대금지급기한": "03",
    "대금지급및 배당기일": "04",
    "배당기일": "05",
    "일부배당": "06",
    "일부배당 및 상계": "07",
    "심문기일": "08",
    "추가배당기일": "09",
    "개찰기일": "11"
}

# 기일 결과 코드 매핑 (가정)
# 실제 코드는 API 응답에서 확인하여 업데이트 필요
AUCTION_RESULT_MAPPING = {
    "매각": "01",
    "유찰": "02",
    "변경": "03",
    "취하": "04",
    "연기": "05",
    "정지": "06",
    "취소": "07",
    "종결": "08",
    "매각불허가": "09"
}


def get_auctions_with_expired_dates():
    """
    경매 기일이 지났지만 결과가 업데이트되지 않은 데이터 조회

    Returns:
        기일이 지난 경매 목록
    """
    today_str = datetime.today().strftime("%Y%m%d")

    # 기일(dxdyYmd)이 오늘보다 이전인데 결과 코드(auctnDxdyRsltCd)가 null인 데이터 조회
    pipeline = [
        {
            "$match": {
                "gdsDspslDxdyLst": {
                    "$elemMatch": {
                        "dxdyYmd": {"$lt": today_str},
                        "auctnDxdyRsltCd": None
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 1,
                "csBaseInfo.userCsNo": 1,  # 사건번호
                "csBaseInfo.cortOfcCd": 1,  # 법원 코드
                "dspslGdsDxdyInfo.dspslGdsSeq": 1,  # 매물 번호
                "gdsDspslDxdyLst": 1  # 경매 일정 리스트
            }
        }
    ]

    expired_auctions = list(auctions_collection.aggregate(pipeline))
    logging.info(f"기일이 지난 미업데이트 경매 데이터 {len(expired_auctions)}건 조회 완료")
    return expired_auctions


def fetch_auction_history(bo_cd, srn_sa_no):
    """
    경매 사건의 기일 내역 조회

    Args:
        bo_cd: 법원 코드
        srn_sa_no: 사건 번호

    Returns:
        기일 내역 리스트 또는 None (오류 발생 시)
    """
    # 커스텀 헤더 설정 (User-Agent 변경)
    custom_headers = HEADERS.copy()
    custom_headers[
        "User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

    data = {
        "dma_srchDxdyDtsLst": {
            "cortOfcCd": bo_cd,
            "csNo": srn_sa_no
        }
    }

    try:
        response = requests.post(AUCTION_HISTORY_URL, headers=custom_headers, json=data)
        response.raise_for_status()
        result = response.json()

        if result["status"] == 200:
            history_list = result.get("data", {}).get("dlt_dxdyDtsLst", [])
            logging.info(f"경매 기일 내역 조회 성공: 사건번호 {srn_sa_no}, 법원 코드 {bo_cd}, 내역 {len(history_list)}건")
            return history_list
        else:
            logging.warning(f"경매 기일 내역 조회 실패: 사건번호 {srn_sa_no}, 법원 코드 {bo_cd}, 메시지: {result.get('message')}")
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"경매 기일 내역 조회 요청 실패: {e}")
        return None


def parse_auction_date(date_time_str):
    """
    날짜 시간 문자열(예: '2023.10.31(10:00)')을 년월일(YYYYMMDD)과 시간(HHMM)으로 분리

    Args:
        date_time_str: 날짜 시간 문자열

    Returns:
        (년월일, 시간) 튜플 또는 (None, None) (파싱 실패 시)
    """
    try:
        # 괄호로 날짜와 시간 분리
        parts = date_time_str.split('(')
        date_part = parts[0]  # '2023.10.31'
        time_part = parts[1].rstrip(')')  # '10:00'

        # 날짜 파싱 (2023.10.31 -> 20231031)
        date_components = date_part.split('.')
        year = date_components[0]
        month = date_components[1]
        day = date_components[2]
        date_str = f"{year}{month}{day}"

        # 시간 파싱 (10:00 -> 1000)
        time_components = time_part.split(':')
        hour = time_components[0]
        minute = time_components[1]
        time_str = f"{hour}{minute}"

        return date_str, time_str

    except (IndexError, ValueError) as e:
        logging.error(f"날짜 시간 파싱 실패: {date_time_str}, 오류: {e}")
        return None, None


def update_auction_with_history(auction, history_list):
    """
    경매 기일 내역으로 DB 업데이트

    Args:
        auction: 업데이트할 경매 정보
        history_list: 경매 기일 내역 리스트

    Returns:
        업데이트 성공 여부
    """
    if not history_list:
        return False

    auction_id = auction["_id"]
    maemul_ser = auction["dspslGdsDxdyInfo"]["dspslGdsSeq"]

    # 기존 일정 리스트 가져오기
    existing_dates = auction.get("gdsDspslDxdyLst", [])

    # 기일 내역을 기존 형식에 맞게 변환
    updated = False

    for history_item in history_list:
        # 매물 번호가 일치하는지 확인
        if history_item.get("dspslGdsSeq") != maemul_ser:
            continue

        # 날짜와 시간 파싱
        date_str, time_str = parse_auction_date(history_item.get("dxdyTime", ""))
        if not date_str or not time_str:
            continue

        # 기일 종류 코드 변환
        kind_name = history_item.get("auctnDxdyKndNm", "")
        kind_code = AUCTION_KIND_MAPPING.get(kind_name)

        if not kind_code:
            logging.warning(f"알 수 없는 기일 종류: {kind_name}")
            continue

        # 경매 결과 확인 및 코드 매핑
        result_str = history_item.get("bidRsltNm", None)
        result_code = None
        if result_str:
            result_code = AUCTION_RESULT_MAPPING.get(result_str)
            if not result_code:
                logging.warning(f"알 수 없는 경매 결과: {result_str}")

        # 낙찰가격 (매각된 경우)
        sale_price = None
        if result_str == "매각":
            # 낙찰가격 문자열에서 숫자만 추출 (예: "187,000,000원" -> 187000000)
            price_str = history_item.get("tsLwsDspslPrc", "0")
            if isinstance(price_str, str):
                price_str = price_str.replace(",", "").replace("원", "")
                try:
                    sale_price = int(price_str)
                except ValueError:
                    logging.warning(f"낙찰가격 변환 실패: {price_str}")

        # 일치하는 기존 일정 찾기
        found = False
        for i, existing_date in enumerate(existing_dates):
            if existing_date.get("dxdyYmd") == date_str and existing_date.get("auctnDxdyKndCd") == kind_code:
                # 기존 일정 업데이트
                if existing_date.get("auctnDxdyRsltCd") != result_code:
                    existing_dates[i]["auctnDxdyRsltCd"] = result_code
                    # 매각된 경우 낙찰가격 추가
                    if result_code == AUCTION_RESULT_MAPPING.get("매각") and sale_price:
                        existing_dates[i]["dspslAmt"] = sale_price
                    updated = True
                found = True
                break

        # 기존 일정에 없으면 새로 추가
        if not found:
            new_date = {
                "dxdyYmd": date_str,
                "dxdyHm": time_str,
                "bidBgngYmd": None,
                "bidEndYmd": None,
                "dxdyPlcNm": history_item.get("dxdyPlcNm", ""),
                "auctnDxdyKndCd": kind_code,
                "auctnDxdyRsltCd": result_code,
                "auctnDxdyGdsStatCd": None,
                "tsLwsDspslPrc": int(history_item.get("aeeEvlAmt", "0") or "0")
            }

            # 매각된 경우 낙찰가격 추가
            if result_code == AUCTION_RESULT_MAPPING.get("매각") and sale_price:
                new_date["dspslAmt"] = sale_price

            existing_dates.append(new_date)
            updated = True

    # 변경된 내용이 있을 때만 DB 업데이트
    if updated:
        auctions_collection.update_one(
            {"_id": auction_id},
            {"$set": {"gdsDspslDxdyLst": existing_dates}}
        )
        logging.info(f"경매 기일 내역 업데이트 완료: ID {auction_id}")
        return True
    else:
        logging.info(f"경매 기일 내역 업데이트 불필요: ID {auction_id}")
        return False


def update_expired_auctions(batch_size=50):
    """
    기일이 지난 경매 데이터 업데이트

    Args:
        batch_size: 한 번에 처리할 데이터 크기 (기본값: 50)
    """
    expired_auctions = get_auctions_with_expired_dates()
    total = len(expired_auctions)
    success_count = 0

    logging.info(f"총 {total}건의 기일 지난 경매 데이터 업데이트 시작")

    # 배치 처리
    for i in range(0, total, batch_size):
        batch = expired_auctions[i:i + batch_size]
        logging.info(f"배치 처리 중: {i + 1} ~ {min(i + batch_size, total)} / {total}")

        for auction in batch:
            time.sleep(1)  # 요청 간격 조정

            bo_cd = auction["csBaseInfo"]["cortOfcCd"]
            srn_sa_no = auction["csBaseInfo"]["userCsNo"]

            # 경매 기일 내역 조회
            history_list = fetch_auction_history(bo_cd, srn_sa_no)

            if history_list:
                # 조회한 내역으로 DB 업데이트
                success = update_auction_with_history(auction, history_list)
                if success:
                    success_count += 1

    logging.info(f"기일 지난 경매 데이터 업데이트 완료: 총 {total}건 중 {success_count}건 성공")


if __name__ == "__main__":
    # 기본 로깅 설정
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # 기일이 지난 경매 데이터 업데이트
    update_expired_auctions()