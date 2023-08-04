import requests
from .stock_screener import StockScreener
from .earnings_releases import EarningsReleaseScraper
from typing import Any, Dict, List
from datetime import datetime


class ZacksScraper:
    def __init__(self, username, password, use_proxy=False):
        self.username = username
        self.password = password
        self.logged_in = False
        self.session = requests.Session()
        self.use_proxy = use_proxy

        # Optional Charles proxy for debugging
        if use_proxy:
            self.session.proxies = {
                "http": "http://localhost:8888",
                "https": "http://localhost:8888",
            }
            self.session.verify = False

        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36"
            }
        )

    def login(self):
        if not self.logged_in:
            login_url = "https://www.zacks.com"

            headers = {
                "content-type": "application/x-www-form-urlencoded",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            }

            params = {
                "force_login": "true",
                "username": self.username,
                "password": self.password,
                "remember_me": "off",
            }

            response = self.session.post(login_url, headers=headers, params=params)

            if response.status_code == 200:
                self.logged_in = True
                return None
            else:
                raise Exception(f"Login status: {response.status_code}")

    def run_stock_screen(self, config: List[Dict[str, Any]]):
        self.login()

        screener = StockScreener(self.session)
        return screener.run(config)

    def scrape_earnings_release(self, timestamp: datetime):
        self.login()

        earnings_release = EarningsReleaseScraper(self.session)
        return earnings_release.scrape(timestamp)
