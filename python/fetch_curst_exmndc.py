import logging
import time

import requests

from config import DETAIL_CURST_URL, HEADERS
from db import is_auction_study_duplicate, save_auction_study


def fetch_curst_exmndc(srn_sa_no, bo_cd):
    """
    물건 상세 조회 후 auction_studies 컬렉션에 저장 (중복 검사 포함)
    """
    if is_auction_study_duplicate(srn_sa_no, bo_cd):
        logging.info(f"이미 존재하는 현황조사서 데이터: 사건번호 {srn_sa_no}, 법원 코드 {bo_cd}")
        return  # 중복 데이터이므로 API 호출하지 않음

    time.sleep(0.1)
    logging.info(f"현황조사서 조회 요청: 사건번호 {srn_sa_no}, 법원 코드 {bo_cd}")

    data = {
        "dma_srchCurstExmn": {
            "cortOfcCd": bo_cd,
            "csNo": srn_sa_no,
            "auctnInfOriginDvsCd": "2",
            "ordTsCnt": None
        }
    }

    try:
        response = requests.post(DETAIL_CURST_URL, headers=HEADERS, json=data)
        response.raise_for_status()
        result = response.json()

        auction_study_data = result.get("data", None)
        if auction_study_data:
            # 참조 정보 추가
            auction_study_data["reference"] = {
                "cortOfcCd": bo_cd,
                "csNo": srn_sa_no
            }

            save_auction_study(auction_study_data)
            logging.info(f"현황조사서 저장 완료: 사건번호 {srn_sa_no}, 법원 코드 {bo_cd}")
        else:
            logging.warning(f"현황조사서 데이터 없음: 사건번호 {srn_sa_no}, 법원 코드 {bo_cd}")

    except requests.exceptions.RequestException as e:
        logging.error(f"현황조사서 조회 요청 실패: {e}")
