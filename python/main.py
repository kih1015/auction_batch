from fetch_list import fetch_auction_data
from python.migrate_to_server import migrate_to_server
from python.update_expired_auctions import update_expired_auctions

if __name__ == "__main__":
    conditions = [
        ("0004601", 0, 14),
        ("0004602", 15, 60)
    ]
    # 신규 경매 데이터 패치
    for condition in conditions:
        fetch_auction_data(*condition)
    # 경매 데이터 업데이트
    update_expired_auctions()
    # 로컬 to 서버 마이그레이션
    migrate_to_server()
