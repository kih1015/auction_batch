import logging
from pymongo import MongoClient

from config import MONGO_URI, DB_NAME, COLLECTION_NAME, AUCTION_IMAGES_COLLECTION

# MongoDB 연결 설정
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
auctions_collection = db[COLLECTION_NAME]
images_collection = db[AUCTION_IMAGES_COLLECTION]

# def is_duplicate(srn_sa_no, maemul_ser, bo_cd):
#     """중복 검사: userCsNo, dspslGdsSeq(숫자 변환), bo_cd 기반"""
#     try:
#         maemul_ser = int(maemul_ser)  # 숫자형 변환
#     except ValueError:
#         return False  # 변환 실패 시 중복 아님
#
#     existing_doc = auctions_collection.find_one({
#         "csBaseInfo.userCsNo": srn_sa_no,
#         "dspslGdsDxdyInfo.dspslGdsSeq": maemul_ser,
#         "csBaseInfo.cortOfcCd": bo_cd
#     })
#     return existing_doc is not None


def save_images(csPicLst, auction_id):
    """`auction_images` 컬렉션에 csPicLst 저장 후 저장된 문서들의 ObjectId 리스트 반환"""
    if not csPicLst:
        return []

    image_docs = []
    for pic in csPicLst:
        image_docs.append({
            "auction_id": auction_id,  # 원본 경매 문서 ID 참조
            "csPicLst": pic  # 원본 구조 유지
        })

    # 여러 개의 문서를 한 번에 삽입하고 `_id` 리스트 반환
    inserted_result = images_collection.insert_many(image_docs)
    return inserted_result.inserted_ids


def save_auction_detail(data, csPicLst):
    """경매 상세 정보를 `auctions` 컬렉션에 저장하고, `auction_images` 컬렉션에 이미지 저장"""
    auction_result = auctions_collection.insert_one(data)
    auction_id = auction_result.inserted_id  # 경매 데이터의 `_id`

    # 이미지 데이터가 있다면 별도 컬렉션에 저장하고, 참조 ID만 auctions에 저장
    if csPicLst:
        image_ids = save_images(csPicLst, auction_id)
        auctions_collection.update_one({"_id": auction_id}, {"$set": {"csPicLst": image_ids}})


# 새로운 컬렉션 설정
auction_studies_collection = db["auction_studies"]


def is_auction_study_duplicate(srn_sa_no, bo_cd):
    """중복 검사: 사건번호(csNo), 법원 코드(cortOfcCd) 기반"""
    existing_doc = auction_studies_collection.find_one({
        "reference.cortOfcCd": bo_cd,
        "reference.csNo": srn_sa_no
    })
    return existing_doc is not None


def save_auction_study(data):
    """물건 상세 정보를 auction_studies 컬렉션에 저장"""
    auction_studies_collection.insert_one(data)


def check_and_update_auction(srn_sa_no, maemul_ser, bo_cd, list_auction_date):
    """
    기존 데이터 확인 및 기일 정보 비교 후 필요시 업데이트

    Args:
        srn_sa_no: 사건번호
        maemul_ser: 매물번호
        bo_cd: 법원코드
        list_auction_date: 목록 API에서 받은 기일 정보 (문자열)

    Returns:
        (bool, bool): (중복 여부, 업데이트 필요 여부)
    """
    try:
        maemul_ser = int(maemul_ser)  # 숫자형 변환
    except ValueError:
        return False, False  # 변환 실패 시 중복 아님

    existing_doc = auctions_collection.find_one({
        "csBaseInfo.userCsNo": srn_sa_no,
        "dspslGdsDxdyInfo.dspslGdsSeq": maemul_ser,
        "csBaseInfo.cortOfcCd": bo_cd
    })

    if not existing_doc:
        return False, False  # 중복 아님, 업데이트 필요 없음

    # 기존 데이터의 dspslGdsDxdyInfo.dspslDxdyYmd 필드와 새 기일 정보 비교
    need_update = False

    if list_auction_date:
        # dspslGdsDxdyInfo에서 dspslDxdyYmd 가져오기
        existing_date = existing_doc.get("dspslGdsDxdyInfo", {}).get("dspslDxdyYmd", "")

        # 기존 기일과 새 기일이 다르면 업데이트 필요
        if existing_date != list_auction_date:
            need_update = True
            logging.info(
                f"기일 정보 불일치 감지: {srn_sa_no}, {maemul_ser}, {bo_cd}, 기존: {existing_date}, 새로운: {list_auction_date}")

    return True, need_update  # (중복임, 업데이트 필요 여부)
