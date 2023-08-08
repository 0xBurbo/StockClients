import requests
from enum import Enum
from datetime import datetime
import json
import pandas as pd
from bs4 import BeautifulSoup


class EarningsCalendarTab(Enum):
    def __str__(self):
        match self.value:
            case 1:
                return "earnings"
            case 9:
                return "sales"
            case 6:
                return "guidance"
            case 3:
                return "revisions"
            case 5:
                return "dividends"
            case 4:
                return "splits"
            case 8:
                return "transcripts"

    EARNINGS = 1
    SALES = 9
    GUIDANCE = 6
    REVISIONS = 3
    DIVIDENDS = 5
    SPLITS = 4
    TRANSCRIPTS = 8


class EarningsCalendarScraper:
    def __init__(self, session: requests.Session):
        self.session = session

    def scrape(self, tab: EarningsCalendarTab, dt: datetime):
        response = self.fetch_tab(tab, dt)

        df = self.parse_tab(response, tab)
        return df

    def fetch_tab(self, tab: EarningsCalendarTab, dt: datetime):
        url = "https://www.zacks.com/includes/classes/z2_class_calendarfunctions_data.php?calltype=eventscal"
        url += f"&date={int(dt.timestamp())}"
        url += f"&type={tab.value}"
        url += "&search_trigger=0"
        url += f"&_={int(datetime.now().timestamp())}"

        response = self.session.get(url)
        if not response.ok:
            raise Exception(
                f"Error fetching tab {tab} for date {dt.ctime()}: status code {response.status_code}"
            )

        return response.text

    def remove_last_bracket(self, s: str):
        index = s.rfind("}")
        # If '}' was found, remove it
        if index != -1:
            s = s[:index] + s[index + 1 :]
        return s

    def parse_tab(self, response: str, tab: EarningsCalendarTab):
        # Extract JSON data from JavaScript request body
        body = response.split('"data" : ', 1)[1]
        body = self.remove_last_bracket(body)
        body = body.strip()
        data = json.loads(body)

        match tab:
            case EarningsCalendarTab.EARNINGS:
                return self.parse_earnings_tab(data)
            case EarningsCalendarTab.SALES:
                # Sales has exact structure as earnings
                return self.parse_earnings_tab(data)
            case EarningsCalendarTab.GUIDANCE:
                return self.parse_guidance_tab(data)
            case EarningsCalendarTab.REVISIONS:
                return self.parse_revisions_tab(data)
            case EarningsCalendarTab.DIVIDENDS:
                return self.parse_dividends_tab(data)
            case EarningsCalendarTab.SPLITS:
                return self.parse_splits_tab(data)

    def parse_earnings_tab(self, data):
        df = pd.DataFrame(
            columns=[
                "symbol",
                "company",
                "mcap",
                "time",
                "estimate",
                "reported",
                "surprise",
                "percent_surprise",
                "percent_price_change",
            ]
        )

        for row in data:
            try:
                newRow = self.parse_earnings_row(row)
                df = pd.concat([df, newRow], ignore_index=True)
            except Exception as e:
                print(f"Error parsing row: {row}")
                print(e)

        return df

    def parse_earnings_row(self, row):
        parsedData = {}

        # Index 0: Symbolƒ
        soup = BeautifulSoup(row[0], "html.parser")
        texts = soup.findAll(text=True, recursive=True)
        parsedData["symbol"] = texts[1]

        # Index 1: Company
        soup = BeautifulSoup(row[1], "html.parser")
        texts = soup.findAll(text=True, recursive=True)
        parsedData["company"] = texts[0]

        # Index 2-7
        parsedData["mcap"] = row[2]
        parsedData["time"] = row[3]
        parsedData["estimate"] = row[4]
        parsedData["reported"] = row[5]

        # TODO: Sometimes these tabs emit data but isn't visible on site,
        # There is some sort of logic for when these should be added or not.
        # parsedData["surprise"] = row[6]
        # parsedData["percent_surprise"] = row[7]
        # parsedData["percent_price_change"] = row[8]

        newRow = pd.Series(data=parsedData)
        newRow = newRow.to_frame().transpose()
        return newRow

    def parse_guidance_tab(self, data):
        df = pd.DataFrame(
            columns=[
                "symbol",
                "company",
                "mcap",
                "period",
                "period_end",
                "guid_range",
                "mid_guid",
                "cons",
                "percent_to_high_point",
            ]
        )

        for row in data:
            try:
                newRow = self.parse_guidance_row(row)
                df = pd.concat([df, newRow], ignore_index=True)
            except Exception as e:
                print(f"Error parsing guidance row: {row}")
                print(e)

        return df

    def parse_guidance_row(self, row):
        parsedData = {}

        # Index 0: Symbol
        soup = BeautifulSoup(row[0], "html.parser")
        texts = soup.findAll(text=True, recursive=True)
        parsedData["symbol"] = texts[1]

        # Index 1: Company
        soup = BeautifulSoup(row[1], "html.parser")
        texts = soup.findAll(text=True, recursive=True)
        parsedData["company"] = texts[0]

        # Other indexs
        parsedData["mcap"] = row[2]
        parsedData["period"] = row[3]
        parsedData["period_end"] = row[4]
        parsedData["guid_range"] = row[5]
        parsedData["mid_guid"] = row[6]
        parsedData["cons"] = row[7]
        parsedData["percent_to_high_point"] = row[8]

        newRow = pd.Series(data=parsedData)
        newRow = newRow.to_frame().transpose()
        return newRow

    def parse_revisions_tab(self, data):
        df = pd.DataFrame(
            columns=[
                "symbol",
                "company",
                "mcap",
                "period",
                "period_end",
                "old",
                "new",
                "est_change",
                "cons",
                "new_est_vs_cons",
            ]
        )

        for row in data:
            try:
                newRow = self.parse_revisions_row(row)
                df = pd.concat([df, newRow], ignore_index=True)
            except Exception as e:
                print(f"Error parsing revisions row: {row}")
                print(e)

        return df

    def parse_revisions_row(self, row):
        parsedData = {}

        # Index 0: Symbol
        soup = BeautifulSoup(row[0], "html.parser")
        texts = soup.findAll(text=True, recursive=True)
        parsedData["symbol"] = texts[1]

        # Index 1: Company
        soup = BeautifulSoup(row[1], "html.parser")
        texts = soup.findAll(text=True, recursive=True)
        parsedData["company"] = texts[0]

        parsedData["mcap"] = row[2]
        parsedData["period"] = row[3]
        parsedData["period_end"] = row[4]
        parsedData["old"] = row[5]
        parsedData["new"] = row[6]

        soup = BeautifulSoup(row[7], "html.parser")
        texts = soup.findAll(text=True, recursive=True)
        parsedData["est_change"] = texts[0]

        parsedData["cons"] = row[8]

        soup = BeautifulSoup(row[9], "html.parser")
        texts = soup.findAll(text=True, recursive=True)
        parsedData["new_est_vs_cons"] = texts[0]

        newRow = pd.Series(data=parsedData)
        newRow = newRow.to_frame().transpose()
        return newRow

    def parse_dividends_tab(self, data):
        df = pd.DataFrame(
            columns=[
                "symbol",
                "company",
                "mcap",
                "amount",
                "yield",
                "ex_div_date",
                "current_price",
                "payable_date",
            ]
        )

        for row in data:
            try:
                newRow = self.parse_dividends_row(row)
                df = pd.concat([df, newRow], ignore_index=True)
            except Exception as e:
                print(f"Error parsing dividends row: {row}")
                print(e)

        return df

    def parse_dividends_row(self, row):
        parsedData = {}

        # Index 0: Symbol
        soup = BeautifulSoup(row[0], "html.parser")
        texts = soup.findAll(text=True, recursive=True)
        parsedData["symbol"] = texts[1]

        # Index 1: Company
        soup = BeautifulSoup(row[1], "html.parser")
        texts = soup.findAll(text=True, recursive=True)
        parsedData["company"] = texts[0]

        parsedData["mcap"] = row[2]
        parsedData["amount"] = row[3]
        parsedData["yield"] = row[4]
        parsedData["ex_div_date"] = row[5]
        parsedData["current_price"] = row[6]
        parsedData["payable_date"] = row[7]

        newRow = pd.Series(data=parsedData)
        newRow = newRow.to_frame().transpose()
        return newRow

    def parse_splits_tab(self, data):
        df = pd.DataFrame(
            columns=[
                "symbol",
                "company",
                "mcap",
                "price",
                "split_factor",
            ]
        )

        for row in data:
            try:
                newRow = self.parse_splits_row(row)
                df = pd.concat([df, newRow], ignore_index=True)
            except Exception as e:
                print(f"Error parsing splits row: {row}")
                print(e)

        return df

    def parse_splits_row(self, row):
        parsedData = {}

        # Index 0: Symbol
        soup = BeautifulSoup(row[0], "html.parser")
        texts = soup.findAll(text=True, recursive=True)
        parsedData["symbol"] = texts[1]

        # Index 1: Company
        soup = BeautifulSoup(row[1], "html.parser")
        texts = soup.findAll(text=True, recursive=True)
        parsedData["company"] = texts[0]

        parsedData["mcap"] = row[2]
        parsedData["price"] = row[3]
        parsedData["split_factor"] = row[4]

        newRow = pd.Series(data=parsedData)
        newRow = newRow.to_frame().transpose()
        return newRow
