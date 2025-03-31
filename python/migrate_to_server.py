from pymongo import MongoClient
from config import MONGO_URI, SERVER_MONGO_URI, DB_NAME, COLLECTION_NAME
import logging
import traceback

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_connection(uri, name=""):
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.server_info()  # 실제 연결 테스트
        logger.info(f"{name} MongoDB 연결 성공")
        return True
    except Exception as e:
        logger.error(f"{name} MongoDB 연결 실패: {str(e)}")
        return False
    finally:
        client.close()

def migrate_collection(collection_name):
    local_client = None
    server_client = None
    
    try:
        # 연결 테스트
        logger.info("MongoDB 연결 테스트 중...")
        if not test_connection(MONGO_URI, "로컬"):
            raise Exception("로컬 MongoDB 연결 실패")
        if not test_connection(SERVER_MONGO_URI, "서버"):
            raise Exception("서버 MongoDB 연결 실패")
            
        # 로컬 MongoDB 연결
        local_client = MongoClient(MONGO_URI)
        local_db = local_client[DB_NAME]
        local_collection = local_db[collection_name]
        
        # 서버 MongoDB 연결
        server_client = MongoClient(SERVER_MONGO_URI)
        server_db = server_client[DB_NAME]
        server_collection = server_db[collection_name]
        
        # 기존 서버 컬렉션 데이터 삭제
        logger.info(f"서버의 {collection_name} 컬렉션 기존 데이터 삭제 중...")
        server_collection.delete_many({})
        
        # 배치 크기 설정
        BATCH_SIZE = 1000
        total_documents = 0
        
        # 배치 단위로 데이터 처리
        while True:
            # 현재 배치의 데이터 가져오기
            batch = list(local_collection.find({}).skip(total_documents).limit(BATCH_SIZE))
            if not batch:
                break
                
            # 서버에 배치 데이터 삽입
            server_collection.insert_many(batch)
            total_documents += len(batch)
            logger.info(f"진행 상황: {total_documents}개의 문서 처리 완료")
        
        if total_documents > 0:
            # ID 보존 검증
            sample_local = local_collection.find_one({})
            if sample_local:
                sample_server = server_collection.find_one({"_id": sample_local["_id"]})
                if sample_server:
                    logger.info(f"ID 보존 확인 완료: 로컬 _id({sample_local['_id']})가 서버에서도 동일하게 유지됨")
                else:
                    logger.warning("ID 보존 실패: 서버에서 동일한 _id를 찾을 수 없음")
            
            logger.info(f"마이그레이션 완료: 총 {total_documents}개의 문서가 성공적으로 이전되었습니다.")
    
    except Exception as e:
        logger.error(f"마이그레이션 중 오류 발생: {str(e)}")
        logger.error(f"상세 에러: {traceback.format_exc()}")
        raise
    
    finally:
        # 연결 종료
        if local_client:
            local_client.close()
        if server_client:
            server_client.close()

def migrate_to_server():
    try:
        # 설정 값 출력
        logger.info(f"로컬 MongoDB URI: {MONGO_URI}")
        logger.info(f"서버 MongoDB URI: {SERVER_MONGO_URI}")
        logger.info(f"데이터베이스: {DB_NAME}")
        
        # auctions 컬렉션 마이그레이션
        logger.info("auctions 컬렉션 마이그레이션 시작...")
        migrate_collection(COLLECTION_NAME)
        
        # auction_studies 컬렉션 마이그레이션
        logger.info("auction_studies 컬렉션 마이그레이션 시작...")
        migrate_collection("auction_studies")
        
        logger.info("모든 컬렉션의 마이그레이션이 완료되었습니다.")
        
    except Exception as e:
        logger.error(f"마이그레이션 실패: {str(e)}")
        logger.error(f"상세 에러: {traceback.format_exc()}")
        
if __name__ == "__main__":
    migrate_to_server()
