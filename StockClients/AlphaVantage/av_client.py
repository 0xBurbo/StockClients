import pandas as pd
import requests
import duckdb


class AlphaVantageClient:
    def __init__(self, api_key):
        self.api_key = api_key

    def get_active_tickers(self):
        url = (
            "https://www.alphavantage.co/query?function=LISTING_STATUS&state=active&apikey="
            + self.api_key
        )

        return pd.read_csv(url)

    def get_delisted_tickers(self):
        url = (
            "https://www.alphavantage.co/query?function=LISTING_STATUS&state=delisted&apikey="
            + self.api_key
        )

        return pd.read_csv(url)

    def get_erd(self, horizon="3month"):
        url = f"https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&horizon={horizon}&apikey={self.api_key}"

        return pd.read_csv(url)

    def get_eps_history(self, ticker):
        try:
            assert ticker is not None
            ticker = ticker.strip().upper()
            BASE_URL = "https://www.alphavantage.co/query?"
            url = f"{BASE_URL}function=EARNINGS&symbol={ticker}&apikey={self.api_key}"
            res = requests.get(url)

            if len(res.json()) > 0:
                keys = list(res.json().keys())
                if "quarterlyEarnings" in keys:
                    tmp = pd.DataFrame(res.json()["quarterlyEarnings"])
                    tic = ticker.replace("-", ".")

                    if len(tmp) > 0:
                        mem_db = duckdb.connect()
                        tmp["hticker"] = tic
                        tmp["hticker"] = tmp["hticker"].str.replace(
                            "-", ".", regex=True
                        )
                        tmp["datadate"] = pd.to_datetime(
                            tmp["fiscalDateEnding"], format="%Y-%m-%d"
                        ).dt.strftime("%Y%m%d")
                        tmp["eadate"] = pd.to_datetime(
                            tmp["reportedDate"], format="%Y-%m-%d"
                        ).dt.strftime("%Y%m%d")
                        df = mem_db.execute(
                            """
                         select distinct a.hticker, a.eadate,a.datadate, a.reportedEPS as av_actual, a.estimatedEPS as av_est,
                         from tmp as a
                         where a.hticker is not null and a.eadate is not null
                         order by hticker, eadate;"""
                        ).df()

                        return df

        except Exception as e:
            print(f"EPS History error for {ticker}: {str(e)}")
