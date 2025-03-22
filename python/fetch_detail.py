import logging
import time

import requests

from config import DETAIL_URL, HEADERS
from db import is_duplicate, save_auction_detail
from utils import address_to_coordinates


def fetch_auction_detail(srn_sa_no, maemul_ser, bo_cd):
    """경매 상세 정보를 조회하여 MongoDB에 저장 (이미지는 auction_images 컬렉션에 저장)"""
    if is_duplicate(srn_sa_no, maemul_ser, bo_cd):
        logging.info(f"이미 존재하는 상세 데이터 (중복 검사 통과): 사건번호 {srn_sa_no}, 매물 번호 {maemul_ser}, 법원 코드 {bo_cd}")
        return  # 중복 데이터이므로 API 호출하지 않음

    time.sleep(1)  # 요청 간격 조정
    logging.info(f"상세 정보 조회 요청: 사건번호 {srn_sa_no}, 매물 번호 {maemul_ser}, 법원 코드 {bo_cd}")

    data = {
        "dma_srchGdsDtlSrch": {
            "csNo": srn_sa_no,
            "cortOfcCd": bo_cd,
            "dspslGdsSeq": maemul_ser,
            "pgmId": "PGJ151F01"
        }
    }

    try:
        response = requests.post(DETAIL_URL, headers=HEADERS, json=data)
        response.raise_for_status()
        result = response.json()

        dma_result = result.get("data", {}).get("dma_result", None)
        if dma_result:
            # `csPicLst` 데이터 추출
            csPicLst = dma_result.pop("csPicLst", [])

            # gdsDspslObjctLst의 첫 번째 항목의 주소를 좌표로 변환
            gds_list = dma_result.get("gdsDspslObjctLst", [])
            if gds_list:
                first_item = gds_list[0]  # 첫 번째 아이템만 사용
                city = first_item.get("adongSdNm")
                district = first_item.get("adongSggNm")
                neighborhood = first_item.get("adongEmdNm")
                lot_number = first_item.get("rprsLtnoAddr")
                riname = first_item.get("adongRiNm", None)

                lat, lon = address_to_coordinates(city, district, neighborhood, riname, lot_number)

                if lat is not None and lon is not None:
                    dma_result["location"] = {
                        "type": "Point",
                        "coordinates": [lon, lat]  # GeoJSON 형식 (경도, 위도)
                    }
                    logging.info(f"좌표 추가 완료: {dma_result['location']}")

            save_auction_detail(dma_result, csPicLst)
            logging.info(f"상세 정보 저장 완료: 사건번호 {srn_sa_no}, 매물 번호 {maemul_ser}, 법원 코드 {bo_cd}, 이미지 개수: {len(csPicLst)}")
        else:
            logging.warning(f"상세 데이터 없음: 사건번호 {srn_sa_no}, 매물 번호 {maemul_ser}, 법원 코드 {bo_cd}")

    except requests.exceptions.RequestException as e:
        logging.error(f"상세 조회 요청 실패: {e}")
