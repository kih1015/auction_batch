from fetch_list import fetch_auction_data

if __name__ == "__main__":
    conditions = [
        ("0004601", 0, 14),
        ("0004602", 15, 60)
    ]
    for condition in conditions:
        fetch_auction_data(*condition)
