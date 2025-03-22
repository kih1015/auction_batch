import logging
import math
import time

import requests

from config import LIST_URL, HEADERS, PAGE_SIZE
from fetch_curst_exmndc import fetch_curst_exmndc  # 물건 상세 조회 추가
from fetch_detail import fetch_auction_detail
from utils import get_date_str


def fetch_auction_data(cortAuctnSrchCondCd, bid_start_days, bid_end_days):
    """법원경매 목록을 조회하고 바로 상세 정보를 검색하여 저장"""
    page_no = 1
    total_count = None

    bid_start_date = get_date_str(bid_start_days)
    bid_end_date = get_date_str(bid_end_days)

    while True:
        time.sleep(0.5)  # 요청 간격 조정
        logging.info(
            f"[{cortAuctnSrchCondCd}] {bid_start_date} ~ {bid_end_date} (페이지 {page_no}) 요청 중...")

        data = {
            "dma_pageInfo": {
                "pageNo": page_no,
                "pageSize": PAGE_SIZE,
                "totalYn": "Y"
            },
            "dma_srchGdsDtlSrchInfo": {
                "bidDvsCd": "000331",
                "cortAuctnSrchCondCd": cortAuctnSrchCondCd,
                "bidBgngYmd": bid_start_date,
                "bidEndYmd": bid_end_date,
                "cortStDvs": "1",
                "statNum": 1
            }
        }

        try:
            response = requests.post(LIST_URL, headers=HEADERS, json=data)
            response.raise_for_status()
            result = response.json()

            if total_count is None:
                total_count = int(result["data"]["dma_pageInfo"].get("totalCnt", 0))

            logging.info(f"현재 페이지: {page_no} / 총 페이지: {math.ceil(total_count / PAGE_SIZE)} , 총 개수: {total_count}")

            items = result["data"].get("dlt_srchResult", [])

            for item in items:
                # 자동차 및 기타 매물인 경우 조회하지 않기
                if item["lclsUtilCd"] == "30000" or item["lclsUtilCd"] == "40000":
                    logging.info("자동차 및 기타 매물: 조회하지 않음")
                    continue

                # ✅ 물건 상세 정보 추가 요청 및 저장 (기일 정보 전달)
                fetch_auction_detail(item["srnSaNo"], item["maemulSer"], item["boCd"], item.get("maeGiil", ""))
                # ✅ 물건 현황조사서 정보 추가 요청 및 저장
                fetch_curst_exmndc(item["srnSaNo"], item["boCd"])

            if page_no * PAGE_SIZE >= total_count:
                logging.info("모든 페이지 수집 완료")
                break

            page_no += 1

        except requests.exceptions.RequestException as e:
            logging.error(f"목록 조회 요청 실패: {e}")
            break
