import requests
from typing import Any, Dict, List
from bs4 import BeautifulSoup
import urllib
import csv
from stock_screener_query import write_query
from util import create_multipart_formdata


class StockScreener:
    def __init__(self, session: requests.Session):
        self.session = session
        self.session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"
            }
        )

    def fetch_stock_screener_page(self) -> Dict[str, Any]:
        url = "https://www.zacks.com/screening/stock-screener"
        response = self.session.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        iframe = soup.find("iframe")

        if not iframe:
            raise Exception("No iframe found on the page")

        src = iframe.get("src")

        if not src:
            raise Exception("No src attribute found in the iframe")

        # Parse the 'src' of iframe
        parsed = urllib.parse.urlparse(src)

        # Extract the query string parameters
        params = urllib.parse.parse_qs(parsed.query)

        # We are interested in 'c_key' parameter
        c_key = params.get("c_key", [None])[0]

        if not c_key:
            raise Exception("No c_key parameter found in the iframe src")

        return {"CKey": c_key}

    def fetch_screener_api(self, parsed: Dict[str, Any]) -> None:
        url = f"https://screener-api.zacks.com/?scr_type=stock&c_id=zacks&c_key={parsed['CKey']}&ref=screening"
        response = self.session.get(url)
        response.raise_for_status()

    def reset_query_params(self) -> None:
        url = "https://screener-api.zacks.com/reset_param.php"
        response = self.session.get(url)
        response.raise_for_status()

    def send_query(self, parameters: List[Dict[str, Any]]) -> None:
        url = "https://screener-api.zacks.com/getrunscreendata.php"

        form_data = write_query(parameters)
        content_type, body = create_multipart_formdata(form_data)

        headers = {
            "content-type": content_type,
        }

        response = self.session.post(url, data=body, headers=headers)
        response.raise_for_status()

    def download_data(self, parsed: Dict[str, Any]) -> List[List[str]]:
        url = "https://screener-api.zacks.com/export.php"
        response = self.session.get(url)
        response.raise_for_status()

        reader = csv.reader(response.text.splitlines())
        return list(reader)

    def run(self, parameters: List[Dict[str, Any]]):
        parsed = self.fetch_stock_screener_page()
        self.fetch_screener_api(parsed)
        self.reset_query_params()
        self.send_query(parameters)
        data = self.download_data(parsed)
        return data
