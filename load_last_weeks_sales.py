from configparser import ConfigParser
import pandas as pd
from config import create_connection_obj

sales_url = "https://www.redfin.com/stingray/api/gis-csv?al=1&include_pending_homes=true&isRentals=false&market=oregon&num_homes=350&ord=redfin-recommended-asc&page_number=1&region_id=30772&region_type=6&sold_within_days=7&status=9&uipt=2&v=8"
storage_options = {"User-Agent": "Mozilla/5.0"}

if __name__ == "__main__":
    conn = create_connection_obj()
    df = pd.read_csv(sales_url, storage_options=storage_options)
    df.to_sql(con=conn, name="last_weeks_sales", index=False, if_exists="append")
    print("Succesfully Added Last Week's Transactions to Database")
