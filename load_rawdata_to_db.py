import os
from configparser import ConfigParser
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


def create_connection_obj():
    """Creates engine object from database.ini configuration"""
    filename = "database.ini"
    parser = ConfigParser()
    parser.read(filename)

    params = {k: v for k, v in parser.items("postgresql")}
    conn = create_engine(URL.create(**params))
    return conn


def load_rawdata(data_folder):
    """Read the Redfin raw data files, concat, drop duplicates, and sanitize column names

    Args:
        data_folder ([str]): path to folder containing all the raw redfin data
    """
    paths = [os.path.join(data_folder, path) for path in os.listdir(data_folder)]
    rawdata = pd.concat([pd.read_csv(path) for path in paths], axis=0)
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
    raw_data_folder = "raw_data"
    data = load_rawdata(data_folder=raw_data_folder)
    data.to_sql(con=conn, name="raw_redfin_data", index=False, if_exists="replace")
    print("Succesfully Added Raw Redfin Data to Database")
