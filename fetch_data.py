from dateutil.relativedelta import relativedelta
import logging
from kiteconnect import KiteConnect
import pandas
import datetime
from os import path
import dateutil as dateutil
from dotenv import load_dotenv
from os import getenv

logging.basicConfig(level=logging.DEBUG)
kite = KiteConnect(api_key=getenv('API_KEY'))


def login():
    url = kite.login_url()
    print(url)
    request_token = input(
        "Enter the request token from the URL in the browser:\n")
    data = kite.generate_session(
        request_token, api_secret=getenv('API_SECRET'))
    kite.set_access_token(data["access_token"])


def get_time_difference_in_months(to_date, from_date):
    difference = relativedelta(to_date, from_date)

    # Get the difference in months
    return difference.years * 12 + difference.months


def get_historical_data_for_daterange(instrument_code, from_date, to_date, timeframe):
    fromDate = datetime.datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
    toDate = datetime.datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")
    temp_to_date = toDate
    temp_from_date = fromDate
    file_name = instrument_code + \
        f'-{fromDate.date().isoformat()}~{toDate.date().isoformat()}-' + '_data.csv'
    while True:

        if get_time_difference_in_months(toDate, temp_from_date) > 6:
            temp_to_date = temp_from_date + \
                relativedelta(months=+6, hours=+6, minutes=+30)
            print(f"From Date: {temp_from_date}")
            print(f"To Date: {temp_to_date}")
            data = kite.historical_data(
                instrument_code, temp_from_date, temp_to_date, timeframe)

            df = pandas.DataFrame(data)

            df.to_csv(file_name, index=False,
                      header=not path.exists(file_name), mode='a')
            temp_from_date = temp_to_date+relativedelta(days=+1)
        else:
            print(f"From Date: {temp_from_date}")
            print(f"To Date: {toDate}")
            data = kite.historical_data(
                instrument_code, temp_from_date, toDate, timeframe)

            df = pandas.DataFrame(data)

            df.to_csv(file_name, index=False,
                      header=not path.exists(file_name), mode='a')
            break
    print("\nData saved to the file!\n")


login()
get_historical_data_for_daterange(
    "260105", "2021-04-27 09:00:00", "2023-04-27 15:30:00", "15minute")
