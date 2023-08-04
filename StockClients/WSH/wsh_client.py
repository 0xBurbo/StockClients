from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List
import requests
import urllib
import xml.etree.ElementTree as ET
import os
import json
import pandas as pd

# For testing, cache responses to avoid hitting the API limit
CACHE_FILENAME = "cache.json"
DateTime2_format = "%m/%d/%Y"


class WSHClient:
    def __init__(self, customer_id, password, cache=False):
        self.customer_id = customer_id
        self.password = password
        self.base_url = "https://enchilada.wallstreethorizon.com/webservice6.asp?"
        self.cache = cache

    def base_params(self):
        return {
            "c": self.customer_id,
            "p": self.password,
        }

    def run_query(self, classes: List[str], date_start, date_end, stock_symbols="*"):
        # WSH Only allows one class per request if stock_symbols is *,
        # so we need to make multiple requests and combine the results
        dfs = defaultdict(list)
        date_range = self.split_date_range(date_start, date_end)

        for dates in date_range:
            for cls in classes:
                params = self.base_params()
                params["stock_symbols"] = stock_symbols
                params["classes"] = cls

                params["from"] = dates[0]
                params["to"] = dates[1]

                params["v"] = "3"
                params["o"] = "EVENTS,EMPTY_TAGS"

                url = self.base_url + urllib.parse.urlencode(params)
                data = self.send_request(url)
                parsed_dfs = self.parse_response(data)

                for parsed_class in parsed_dfs:
                    dfs[parsed_class].append(parsed_dfs[parsed_class])

        merged_dfs = self.merge_class_dfs(dfs)
        return merged_dfs

    def load_cache(self):
        cache = {}
        if os.path.exists(CACHE_FILENAME):
            with open(CACHE_FILENAME, "r") as f:
                cache = json.load(f)
        return cache

    # Send request, or return cached response if available
    def send_request(self, url: str):
        cache = {}
        if self.cache:
            cache = self.load_cache()
            if url in cache:
                return cache[url]

        res = requests.get(url)

        if self.cache:
            if res.status_code == 200:
                cache[url] = res.text
                with open(CACHE_FILENAME, "w") as f:
                    json.dump(cache, f)

        return res.text

    def parse_response(self, data):
        root = ET.fromstring(data)

        entries = defaultdict(list)

        # TODO: Raise error for error responses

        # Create a dict of lists for each event class
        for item in root:
            data = {}
            for child in item:
                if child.text is None:
                    data[child.tag] = None
                else:
                    data[child.tag] = child.text.encode("utf-8")

            entries[item.tag].append(data)

        # Convert each event class list to a DataFrame
        dfs = {}
        for cls in entries:
            df = pd.DataFrame(entries[cls])
            # Common data types across all classes
            df = df.astype(
                {
                    "event_id": str,
                    "company_id": int,
                    "stock_symbol": str,
                    "isin": str,
                    "company_name": str,
                }
            )

            # Decode bytes to string
            df["created"] = df["created"].str.decode("utf-8")
            df["updated"] = df["updated"].str.decode("utf-8")
            df["return_time"] = df["return_time"].str.decode("utf-8")

            self.convert_datetime2(df, "created")
            self.convert_datetime2(df, "updated")
            self.convert_datetime2(df, "return_time")

            # Class-specific data frame columns
            if cls == "db":
                df = df.astype(
                    {
                        "stock_exchange": str,
                        "quarter": str,
                        "fiscal_year": int,
                        "echangetype": str,
                        "prior_earnings_date": str,
                        "earnings_date": str,
                        "time_of_day": str,
                        "earnings_date_status": str,
                        "total_days_changed": str,  # Int in API but there are missing entries
                        "confidence_indicator": str,
                        "confirmed_date_zscore": float,
                        "quarter_end_date": str,
                        "audit_source": str,
                        "prelim_earnings_date": str,
                        "option_expiration_date": str,
                        "option_expiration_code": str,
                        "filing_due_date": str,
                        "announcement_url": str,
                        # "announce_datetime": str, # Only returned in v4
                        "change_reason": str,
                        "disclaimer": str,
                        "same_store_sales": str,
                    }
                )

                self.convert_date(df, "prior_earnings_date")
                self.convert_date(df, "earnings_date")
                self.convert_date(df, "quarter_end_date")
                self.convert_date(df, "prelim_earnings_date")
                self.convert_date(df, "option_expiration_date")
                self.convert_date(df, "filing_due_date")
                # self.convert_date(df, "announce_datetime") # API v4 only
            elif cls == "ed":
                df = df.astype(
                    {
                        "companies.stock_exchange": str,
                        "earnings_date": str,
                        "quarter": str,
                        "fiscal_year": int,
                        "earnings_date_status": str,
                        "time_of_day": str,
                        "prelim_earnings_date": str,
                        "quarter_end_date": str,
                        "audit_source": str,
                        "filing_due_date": str,
                        "announcement_url": str,
                        "announce_datetime": str,
                        "disclaimer": str,
                    }
                )
                self.convert_date(df, "earnings_date")
                self.convert_date(df, "prelim_earnings_date")
                self.convert_date(df, "quarter_end_date")
                self.convert_date(df, "filing_due_date")

            dfs[cls] = df

        return dfs

    def merge_class_dfs(self, dataframes: Dict[str, List[pd.DataFrame]]):
        merged_dataframes = {}
        for cls, dfs in dataframes.items():
            # Merge all dataframes within this class
            df_class_merged = pd.concat(dfs, ignore_index=True)
            merged_dataframes[cls] = df_class_merged
            
        return merged_dataframes

    # def merge_class_dfs(self, dataframes: Dict[str, List[pd.DataFrame]]):
    #     common_cols = [
    #         "event_id",
    #         "company_id",
    #         "stock_symbol",
    #         "isin",
    #         "company_name",
    #         "class",
    #         "created",
    #         "updated",
    #         "return_time",
    #     ]

    #     # Merge dfs of each class into one df
    #     merged_dataframes = []
    #     for class_name, dfs in dataframes.items():
    #         # Merge all dataframes within this class
    #         df_class_merged = pd.concat(dfs, ignore_index=True)

    #         # Rename columns, excluding common columns
    #         for column in df_class_merged.columns:
    #             if column not in common_cols:
    #                 df_class_merged = df_class_merged.rename(
    #                     columns={column: f"{column}_{class_name}"}
    #                 )

    #         # Add merged dataframe to list
    #         merged_dataframes.append(df_class_merged)

    #     df_final = merged_dataframes[0]
    #     for df in merged_dataframes[1:]:
    #         df_final = df_final.merge(df, how="outer", on=common_cols)

    #     return df_final

    def convert_datetime2(self, df, column, tz="EST"):
        df[column] = pd.to_datetime(
            df[column], errors="coerce", format="%m/%d/%Y %H:%M:%S %p"
        )
        df[column] = df[column].dt.tz_localize(tz)
        df[column] = df[column].dt.strftime("%Y%m%d%H%M%S")

    def convert_date(self, df, column, tz="EST"):
        df[column] = pd.to_datetime(df[column], errors="coerce", format="%m/%d/%Y")
        df[column] = df[column].dt.tz_localize(tz)
        df[column] = df[column].dt.strftime("%Y%m%d")

    def split_date_range(self, start_date, end_date, max_days=7):
        start_date = datetime.strptime(start_date, "%m/%d/%Y")
        end_date = datetime.strptime(end_date, "%m/%d/%Y")

        interval_start = start_date
        intervals = []

        while interval_start < end_date:
            interval_end = min(interval_start + timedelta(days=max_days), end_date)
            intervals.append(
                (interval_start.strftime("%m/%d/%Y"), interval_end.strftime("%m/%d/%Y"))
            )
            interval_start = interval_end + timedelta(
                days=1
            )  # start next interval on next day

        return intervals
