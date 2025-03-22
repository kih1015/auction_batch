import logging
import time

import requests

from config import DETAIL_URL, HEADERS
from db import check_and_update_auction, save_auction_detail, save_images, auctions_collection, images_collection
from utils import address_to_coordinates


def fetch_auction_detail(srn_sa_no, maemul_ser, bo_cd, list_auction_date=None):
    """
    경매 상세 정보를 조회하여 MongoDB에 저장 (이미지는 auction_images 컬렉션에 저장)
    list_auction_date: 목록 API에서 받은 기일 정보
    """
    # 중복 및 업데이트 필요 여부 확인
    is_duplicate, need_update = check_and_update_auction(srn_sa_no, maemul_ser, bo_cd, list_auction_date)

    if is_duplicate and not need_update:
        logging.info(f"이미 존재하는 상세 데이터 (중복 검사 통과): 사건번호 {srn_sa_no}, 매물 번호 {maemul_ser}, 법원 코드 {bo_cd}")
        return  # 중복 데이터이므로 API 호출하지 않음

    time.sleep(1)  # 요청 간격 조정

    if is_duplicate and need_update:
        logging.info(f"기일 정보 변경으로 상세 정보 재조회: 사건번호 {srn_sa_no}, 매물 번호 {maemul_ser}, 법원 코드 {bo_cd}")
    else:
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

            # 기존 문서가 있고 업데이트가 필요한 경우 (기일 변경)
            if is_duplicate and need_update:
                # 기존 이미지 참조 가져오기
                existing_doc = auctions_collection.find_one({
                    "csBaseInfo.userCsNo": srn_sa_no,
                    "dspslGdsDxdyInfo.dspslGdsSeq": int(maemul_ser),
                    "csBaseInfo.cortOfcCd": bo_cd
                })

                # 기존 이미지 ID 가져오기 및 삭제
                old_image_ids = existing_doc.get("csPicLst", [])

                # ID 값으로 기존 이미지 삭제
                for image_id in old_image_ids:
                    images_collection.delete_one({"_id": image_id})

                logging.info(f"기존 이미지 {len(old_image_ids)}개 삭제 완료")

                # 새 이미지 저장 및 ID 업데이트
                image_ids = save_images(csPicLst, existing_doc["_id"])
                dma_result["csPicLst"] = image_ids

                # 문서 업데이트
                auctions_collection.update_one(
                    {"_id": existing_doc["_id"]},
                    {"$set": dma_result}
                )
                logging.info(
                    f"기일 변경으로 상세 정보 업데이트 완료: 사건번호 {srn_sa_no}, 매물 번호 {maemul_ser}, 법원 코드 {bo_cd}, 이미지 개수: {len(csPicLst)}")
            else:
                # 새 문서 저장
                save_auction_detail(dma_result, csPicLst)
                logging.info(
                    f"상세 정보 저장 완료: 사건번호 {srn_sa_no}, 매물 번호 {maemul_ser}, 법원 코드 {bo_cd}, 이미지 개수: {len(csPicLst)}")
        else:
            logging.warning(f"상세 데이터 없음: 사건번호 {srn_sa_no}, 매물 번호 {maemul_ser}, 법원 코드 {bo_cd}")

    except requests.exceptions.RequestException as e:
        logging.error(f"상세 조회 요청 실패: {e}")