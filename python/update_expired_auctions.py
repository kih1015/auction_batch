import logging
import time
from datetime import datetime

import requests
from pymongo import MongoClient

from config import MONGO_URI, DB_NAME, COLLECTION_NAME, HEADERS

# MongoDB 설정
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
auctions_collection = db[COLLECTION_NAME]

# API 상수
AUCTION_HISTORY_URL = "https://www.courtauction.go.kr/pgj/pgj15A/selectCsDtlDxdyDts.on"

# 코드 매핑
AUCTION_KIND_MAPPING = {
    "매각기일": "01", "매각결정기일": "02", "대금지급기한": "03", "대금지급및 배당기일": "04",
    "배당기일": "05", "일부배당": "06", "일부배당 및 상계": "07", "심문기일": "08",
    "추가배당기일": "09", "개찰기일": "11"
}

AUCTION_RESULT_MAPPING = {
    "매각준비": "000", "매각": "001", "유찰": "002", "최고가매각허가결정": "003",
    "차순위매각허가결정": "004", "최고가매각불허가결정": "005", "차순위매각불허가결정": "006",
    "기한변경": "007", "추후지정": "008", "납부": "009", "미납": "010",
    "기한후납부": "011", "상계허가": "012", "진행": "013", "변경": "014",
    "배당종결": "015", "배당불가": "016", "최고가매각허가취소결정": "017", "차순위매각허가취소결정": "018"
}


def get_auctions_with_expired_dates():
    """경매 기일이 지났지만 결과가 업데이트되지 않은 데이터 조회"""
    today_str = datetime.today().strftime("%Y%m%d")

    pipeline = [
        {
            "$match": {
                "gdsDspslDxdyLst": {
                    "$elemMatch": {
                        "dxdyYmd": {"$lt": today_str},
                        "auctnDxdyRsltCd": None
                    }
                },
                # 이미 취소 처리된 경매는 제외
                "isAuctionCancelled": {"$ne": True}
            }
        },
        {
            "$project": {
                "_id": 1,
                "csBaseInfo.csNo": 1,
                "csBaseInfo.cortOfcCd": 1,
                "dspslGdsDxdyInfo.dspslGdsSeq": 1,
                "gdsDspslDxdyLst": 1
            }
        }
    ]

    expired_auctions = list(auctions_collection.aggregate(pipeline))
    logging.info(f"기일이 지난 미업데이트 경매 데이터 {len(expired_auctions)}건 조회 완료")
    return expired_auctions


def fetch_auction_history(bo_cd, srn_sa_no):
    """경매 사건의 기일 내역 조회"""
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

        logging.warning(f"경매 기일 내역 조회 실패: 사건번호 {srn_sa_no}, 법원 코드 {bo_cd}, 메시지: {result.get('message')}")
        return None

    except requests.exceptions.RequestException as e:
        logging.error(f"경매 기일 내역 조회 요청 실패: {e}")
        return None


def parse_auction_date(date_time_str):
    """날짜 시간 문자열 파싱 (예: '2023.10.31(10:00)')"""
    try:
        parts = date_time_str.split('(')
        date_part = parts[0]
        time_part = parts[1].rstrip(')')

        date_components = date_part.split('.')
        date_str = f"{''.join(date_components)}"

        time_components = time_part.split(':')
        time_str = f"{''.join(time_components)}"

        return date_str, time_str

    except (IndexError, ValueError) as e:
        logging.error(f"날짜 시간 파싱 실패: {date_time_str}, 오류: {e}")
        return None, None


def extract_sale_price(price_str):
    """낙찰가격 문자열에서 숫자만 추출"""
    if not isinstance(price_str, str):
        return None

    try:
        return int(price_str.replace(",", "").replace("원", ""))
    except ValueError:
        logging.warning(f"낙찰가격 변환 실패: {price_str}")
        return None


def update_existing_date(existing_date, result_code, sale_price):
    """기존 일정 데이터 업데이트"""
    updated = False

    if existing_date.get("auctnDxdyRsltCd") != result_code:
        existing_date["auctnDxdyRsltCd"] = result_code
        updated = True

    if result_code == AUCTION_RESULT_MAPPING.get("매각") and sale_price:
        existing_date["dspslAmt"] = sale_price
        updated = True

    return updated


def create_new_date_entry(date_str, time_str, history_item, kind_code, result_code, sale_price):
    """새 기일 데이터 생성"""
    # tsLwsDspslPrc 문자열에서 숫자만 추출
    ts_lws_dspsl_prc_str = history_item.get("tsLwsDspslPrc", "0")
    ts_lws_dspsl_prc = extract_sale_price(ts_lws_dspsl_prc_str) if ts_lws_dspsl_prc_str else 0

    new_date = {
        "dxdyYmd": date_str,
        "dxdyHm": time_str,
        "bidBgngYmd": None,
        "bidEndYmd": None,
        "dxdyPlcNm": history_item.get("dxdyPlcNm", ""),
        "auctnDxdyKndCd": kind_code,
        "auctnDxdyRsltCd": result_code,
        "auctnDxdyGdsStatCd": None,
        "tsLwsDspslPrc": ts_lws_dspsl_prc
    }

    if result_code == AUCTION_RESULT_MAPPING.get("매각") and sale_price:
        new_date["dspslAmt"] = sale_price

    return new_date


def mark_auction_as_cancelled(auction_id):
    """경매를 취소 처리하는 함수"""
    # 취소 처리 필드 추가 및 취소 시간 기록
    update_result = auctions_collection.update_one(
        {"_id": auction_id},
        {
            "$set": {
                "isAuctionCancelled": True,
                "cancelledAt": datetime.now(),
                "cancelReason": "기일 내역 조회 불가"
            }
        }
    )

    if update_result.modified_count > 0:
        logging.info(f"경매 취소 처리 완료: ID {auction_id}")
        return True

    logging.warning(f"경매 취소 처리 실패: ID {auction_id}")
    return False


def update_auction_with_history(auction, history_list):
    """경매 기일 내역으로 DB 업데이트"""
    auction_id = auction["_id"]

    # 기일 내역이 없는 경우 취소 처리
    if not history_list:
        return mark_auction_as_cancelled(auction_id)

    maemul_ser = auction["dspslGdsDxdyInfo"]["dspslGdsSeq"]
    new_dates = []  # 새로운 기일 내역을 담을 리스트

    # 기일 내역 처리
    for history_item in history_list:
        date_entry = process_history_item(history_item, maemul_ser)
        if date_entry:
            new_dates.append(date_entry)

    # 기일 내역이 있으면 DB에 덮어쓰기
    if new_dates:
        save_auction_dates(auction_id, new_dates)
        return True
    else:
        logging.warning(f"매칭되는 기일 내역 없음: ID {auction_id}")
        return False


def process_history_item(history_item, maemul_ser):
    """개별 기일 내역 항목 처리"""
    # 매물 번호 일치 확인
    dspsl_gds_seq = history_item.get("dspslGdsSeq")
    if dspsl_gds_seq is None or int(dspsl_gds_seq) != maemul_ser:
        return None

    # 날짜/시간 및 코드 변환
    date_str, time_str = parse_auction_date(history_item.get("dxdyTime", ""))
    if not date_str or not time_str:
        return None

    kind_name = history_item.get("auctnDxdyKndNm", "")
    kind_code = AUCTION_KIND_MAPPING.get(kind_name)
    if not kind_code:
        logging.warning(f"알 수 없는 기일 종류: {kind_name}")
        return None

    # 결과 정보 처리
    result_info = extract_result_info(history_item)
    if not result_info:
        return None

    result_code, sale_price = result_info

    # 새 기일 항목 생성
    return create_new_date_entry(date_str, time_str, history_item, kind_code, result_code, sale_price)


def extract_result_info(history_item):
    """경매 결과 정보 추출"""
    result_str = history_item.get("dxdyRslt", "")

    # 매각 결과에서 실제 판매 가격 추출
    sale_price = None
    if "매각" in result_str:
        import re
        price_match = re.search(r'(\d[\d,]+)원', result_str)
        if price_match:
            sale_price = extract_sale_price(price_match.group(1))

    # 결과 코드 매핑 - "매각<br>..." 등의 포맷 처리
    clean_result = result_str.split("<")[0] if "<" in result_str else result_str
    result_code = AUCTION_RESULT_MAPPING.get(clean_result) if clean_result else None

    return result_code, sale_price


def save_auction_dates(auction_id, new_dates):
    """경매 기일 내역을 DB에 저장"""
    auctions_collection.update_one(
        {"_id": auction_id},
        {"$set": {"gdsDspslDxdyLst": new_dates}}
    )
    logging.info(f"경매 기일 내역 갱신 완료: ID {auction_id}, 항목 수 {len(new_dates)}개")


def update_expired_auctions(batch_size=50):
    """기일이 지난 경매 데이터 업데이트"""
    expired_auctions = get_auctions_with_expired_dates()
    total = len(expired_auctions)
    success_count = 0
    cancelled_count = 0

    logging.info(f"총 {total}건의 기일 지난 경매 데이터 업데이트 시작")

    for i in range(0, total, batch_size):
        batch = expired_auctions[i:i + batch_size]
        logging.info(f"배치 처리 중: {i + 1} ~ {min(i + batch_size, total)} / {total}")

        for auction in batch:
            time.sleep(0.5)  # 요청 간격 조정
            bo_cd = auction["csBaseInfo"]["cortOfcCd"]
            srn_sa_no = auction["csBaseInfo"]["csNo"]

            history_list = fetch_auction_history(bo_cd, srn_sa_no)

            if not history_list:
                # 기일 내역이 없는 경우 취소 처리
                if mark_auction_as_cancelled(auction["_id"]):
                    cancelled_count += 1
            elif update_auction_with_history(auction, history_list):
                success_count += 1

    logging.info(f"기일 지난 경매 데이터 업데이트 완료: 총 {total}건 중 {success_count}건 성공, {cancelled_count}건 취소 처리")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    update_expired_auctions()