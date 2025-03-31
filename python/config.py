import logging
import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

# .env 파일 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_parameter(param_name, with_decryption=True):
    """
    파라미터 스토어에서 값을 가져오는 함수

    Args:
        param_name: 파라미터 이름
        with_decryption: 암호화된 값 복호화 여부

    Returns:
        파라미터 값 또는 오류 발생 시 None
    """
    try:
        # 환경 변수에서 AWS 자격 증명 로드
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        region = os.environ.get('AWS_REGION')

        if not aws_access_key_id or not aws_secret_access_key:
            logger.error("AWS 자격 증명이 설정되지 않았습니다")
            return None

        ssm = boto3.client(
            'ssm',
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        response = ssm.get_parameter(
            Name=param_name,
            WithDecryption=with_decryption
        )
        return response['Parameter']['Value']
    except ClientError as e:
        logger.error(f"파라미터 {param_name} 가져오기 실패: {e}")
        return None


# 파라미터 스토어에서 민감한 정보 가져오기
MONGO_URI = "mongodb://localhost:27017/"
SERVER_MONGO_URI = get_parameter("/MONGO_URL") or MONGO_URI
KAKAO_REST_API_KEY = get_parameter("/KAKAO_REST_API_KEY")

# 나머지 설정
DB_NAME = "apt"
COLLECTION_NAME = "auctions"
AUCTION_IMAGES_COLLECTION = "auction_images"

# API URL 설정
LIST_URL = "https://www.courtauction.go.kr/pgj/pgjsearch/searchControllerMain.on"
DETAIL_URL = "https://www.courtauction.go.kr/pgj/pgj15B/selectAuctnCsSrchRslt.on"
DETAIL_CURST_URL = "https://www.courtauction.go.kr/pgj/pgj15B/selectCurstExmndc.on"

# 요청 헤더 설정
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json;charset=UTF-8",
    "Host": "www.courtauction.go.kr",
    "Origin": "https://www.courtauction.go.kr",
    "Referer": "https://www.courtauction.go.kr/pgj/index.on?w2xPath=/pgj/ui/pgj100/PGJ151F00.xml",
    "SC-Userid": "NONUSER",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# 페이지 크기 설정
PAGE_SIZE = 40

if __name__ == "__main__":
    api_key = get_parameter("/KAKAO_REST_API_KEY") or os.environ.get("KAKAO_REST_API_KEY", "")
    if api_key:
        print(f"KAKAO_REST_API_KEY가 성공적으로 로드되었습니다")
    else:
        print(f"KAKAO_REST_API_KEY를 가져오지 못했습니다")
