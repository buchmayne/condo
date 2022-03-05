from configparser import ConfigParser
import pandas as pd
from config import create_connection_obj

sales_url = "https://www.redfin.com/stingray/api/gis-csv?al=1&include_pending_homes=true&isRentals=false&market=oregon&num_homes=350&ord=redfin-recommended-asc&page_number=1&region_id=30772&region_type=6&sold_within_days=7&status=9&uipt=2&v=8"
storage_options = {"User-Agent": "Mozilla/5.0"}


def load_recent_sales_data(url, storage_options):
    """Read the Redfin raw data from url, concat, drop duplicates, and sanitize column names

    Args:
        url ([str]): path to last week's Redfin Portland condo sales download

        storage_options ([dict]): Headers for get request
    """
    rawdata = pd.read_csv(url, storage_options=storage_options)
    rawdata = rawdata.drop_duplicates()
    renamer = {
        "SALE TYPE": "SALE_TYPE",
        "SOLD DATE": "SOLD_DATE",
        "PROPERTY TYPE": "PROPERTY_TYPE",
        "STATE OR PROVINCE": "STATE",
        "ZIP OR POSTAL CODE": "ZIPCODE",
        "SQUARE FEET": "SQUARE_FEET",
        "LOT SIZE": "LOT_SIZE",
        "YEAR BUILT": "YEAR_BUILT",
        "DAYS ON MARKET": "DAYS_ON_MARKET",
        "$/SQUARE FEET": "PRICE_PER_SQUARE_FEET",
        "HOA/MONTH": "HOA_PER_MONTH",
        "NEXT OPEN HOUSE START TIME": "NEXT_OPEN_HOUSE_START_TIME",
        "NEXT OPEN HOUSE END TIME": "NEXT_OPEN_HOUSE_END_TIME",
        "URL (SEE https://www.redfin.com/buy-a-home/comparative-market-analysis FOR INFO ON PRICING)": "URL",
        "MLS#": "MLS",
    }

    rawdata = rawdata.rename(columns=renamer)
    return rawdata


if __name__ == "__main__":
    conn = create_connection_obj()
    df = load_recent_sales_data(url=sales_url, storage_options=storage_options)
    df.to_sql(con=conn, name="last_weeks_sales", index=False, if_exists="append")
    print("Succesfully Added Last Week's Transactions to Database")
