import requests
import enum
import datetime
import json
from bs4 import BeautifulSoup
import pandas as pd


class EarningsReleaseTab(enum.Enum):
    ALL = 1
    PLUS_EARNINGS_SURPRISE = 2
    MINUS_EARNINGS_SURPRISE = 3
    PLUS_SALES_SURPRISE = 4
    MINUS_SALES_SURPRISE = 5


earnings_columns = {
    "ticker": "hticker",
    "report_time": "eatime",
    "estimate": "eps_est",
    "reported": "eps_actual",
}

sales_columns = {
    "ticker": "hticker",
    "report_time": "eatime",
    "estimate": "sales_est",
    "reported": "sales_actual",
}


class EarningsReleaseScraper:
    def __init__(self, session: requests.Session):
        self.session = session

    def scrape(self, timestamp: datetime.datetime):
        # Which tabs to scrape
        jobs = [
            EarningsReleaseTab.ALL,
            EarningsReleaseTab.PLUS_EARNINGS_SURPRISE,
            EarningsReleaseTab.MINUS_EARNINGS_SURPRISE,
            EarningsReleaseTab.PLUS_SALES_SURPRISE,
            EarningsReleaseTab.MINUS_SALES_SURPRISE,
        ]

        earnings_df = pd.DataFrame()
        sales_df = pd.DataFrame()

        for job in jobs:
            response = self.fetch_tab(job, timestamp)
            parsed = self.parse_response(response)

            # Rename columns based on which tab was scraped
            if (
                job == EarningsReleaseTab.ALL
                or job == EarningsReleaseTab.PLUS_EARNINGS_SURPRISE
                or job == EarningsReleaseTab.MINUS_EARNINGS_SURPRISE
            ):
                parsed = parsed.rename(columns=earnings_columns)
                earnings_df = pd.concat([earnings_df, parsed], ignore_index=True)

            if (
                job == EarningsReleaseTab.PLUS_SALES_SURPRISE
                or job == EarningsReleaseTab.MINUS_SALES_SURPRISE
            ):
                parsed = parsed.rename(columns=sales_columns)
                sales_df = pd.concat([sales_df, parsed], ignore_index=True)

        # Merge results
        df = pd.merge(earnings_df, sales_df, how="outer", on=["hticker", "eatime"])
        df["eadate"] = timestamp.date().strftime("%Y%m%d")

        # Rearrange columns
        cols = df.columns.tolist()
        cols.insert(1, cols.pop(cols.index("eadate")))
        df = df[cols]

        # Dedupe
        df = df.drop_duplicates()

        return df

    def fetch_tab(self, tab: EarningsReleaseTab, timestamp: datetime.datetime):
        now = int(datetime.datetime.now().timestamp())
        timestampUnix = int(timestamp.timestamp())
        url = f"https://www.zacks.com/research/earnings/z2_earnings_tab_data.php?type={tab}&timestamp={timestampUnix}&_={now}"

        response = self.session.get(url)
        response.raise_for_status()

        return response.text

    def remove_last_bracket(self, s: str):
        index = s.rfind("}")
        # If '}' was found, remove it
        if index != -1:
            s = s[:index] + s[index + 1 :]
        return s

    def parse_response(self, response: str):
        # Extract JSON data from JavaScript request body
        body = response.split('"data"  : ', 1)[1]
        body = self.remove_last_bracket(body)
        body = body.strip()
        data = json.loads(body)

        # Parse data into a Pandas DataFrame
        df = pd.DataFrame(columns=["ticker", "report_time", "estimate", "reported"])

        for row in data:
            try:
                newRow = self.parseRow(row)
                df = pd.concat([df, newRow], ignore_index=True)
            except Exception as e:
                print(f"Error parsing row: {row}")
                print(e)

        return df

    def parseRow(self, row):
        parsedData = {}
        for key in row:
            value = row[key]

            # Zacks data comes wrapped in HTML tags
            # Use BeautifulSoup parse the tags and remove them
            match key:
                case "ticker":
                    soup = BeautifulSoup(value, "html.parser")
                    texts = soup.findAll(text=True, recursive=True)
                    ticker = texts[1]
                    parsedData["ticker"] = ticker
                # case "company_name":
                #     soup = BeautifulSoup(value, "html.parser")
                #     texts = soup.findAll(text=True, recursive=True)
                #     company_name = texts[0]
                #     parsedData['cname'] = company_name
                case "report_time":
                    try:
                        dt = datetime.datetime.strptime(value, "%H:%M")
                        parsedData["report_time"] = dt.time()
                    except ValueError:
                        print(f"Error parsing report_time: {value}")
                        parsedData["report_time"] = None
                case "estimate":
                    parsedData["estimate"] = value
                case "reported":
                    parsedData["reported"] = value
                # case "surprise":
                #     soup = BeautifulSoup(value, "html.parser")
                #     texts = soup.findAll(text=True, recursive=True)
                #     parsedData['surprise'] = texts[0]
                # case "perc_change":
                #     soup = BeautifulSoup(value, "html.parser")
                #     texts = soup.findAll(text=True, recursive=True)
                #     parsedData['percent_change'] = texts[0]

        newRow = pd.Series(data=parsedData)
        newRow = newRow.to_frame().transpose()
        return newRow
