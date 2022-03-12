import os
from datetime import date
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
from joblib import dump
from config import create_connection_obj


best_params_ = {"eta": 0.1, "reg_lambda": 1, "max_depth": 6, "random_state": 1}


# functions
def rmse(y, yhat):
    return np.sqrt(mean_squared_error(y, yhat))


def dummy_encode_zipcode(df):
    most_frequent_zip_codes = [97209, 97201, 97239, 97217, 97210, 97219]

    for zipcode in most_frequent_zip_codes:
        df["zip_" + str(zipcode)] = 1 * (df["ZIPCODE"] == zipcode)
    return df


def dummy_encode_address_direction(df):
    return pd.concat(
        [df, pd.get_dummies(df["address_direction"], drop_first=False)], axis=1
    ).drop(["E", "N", "W", "S", "address_direction"], axis=1)


def process_data_for_model(df):
    df = (
        df.dropna()
    )  # Drop missing values since they're so few and look randomly distributed

    return (
        df.assign(
            months_since_2021=np.where(
                df["SOLD_YEAR"] == 2022, df["SOLD_MONTH"] + 12, df["SOLD_MONTH"]
            )
        )  # Standardize sale date to numeric
        .pipe(
            dummy_encode_address_direction
        )  # Make dummy variables for address direction
        .pipe(dummy_encode_zipcode)  # dummy encode the 6 most frequent zipcodes
        .drop(
            [
                "index",
                "SOLD_YEAR",
                "SOLD_MONTH",
                "ADDRESS",
                "ZIPCODE",
                "address_building",
            ],
            axis=1,
        )  # drop unnecessary columns
    )


if __name__ == "__main__":
    output_path_model = "models/model_{}.joblib".format(
        date.today().strftime("%Y-%m-%d")
    )

    conn = create_connection_obj()
    df = pd.read_sql(sql="SELECT * FROM tidyredfin_data;", con=conn)

    model_data = process_data_for_model(df)

    X = model_data.drop(["PRICE", "PRICE_PER_SQUARE_FEET"], axis=1)
    y = model_data["PRICE_PER_SQUARE_FEET"]

    xgb_model = xgb.XGBRegressor(**best_params_)

    xgb_pipeline = Pipeline([("scaler", StandardScaler()), ("model", xgb_model)])

    xgb_pipeline.fit(X, y)
    dump(xgb_pipeline, output_path_model)

    predicted_sales_price = xgb_pipeline.predict(X)

    print(f"RMSE: {rmse(y, predicted_sales_price).round()}")

    (
        model_data.assign(predicted_sales_price_per_sqft=predicted_sales_price).to_sql(
            con=conn,
            name="model_data_with_predicted_sales_price",
            index=False,
            if_exists="replace",
        )
    )

    print("Model Results Logged to DB")
