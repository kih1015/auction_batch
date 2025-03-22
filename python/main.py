from fetch_list import fetch_auction_data
from python.update_expired_auctions import update_expired_auctions

if __name__ == "__main__":
    conditions = [
        ("0004601", 0, 14),
        ("0004602", 15, 60)
    ]
    for condition in conditions:
        fetch_auction_data(*condition)

    update_expired_auctions()
