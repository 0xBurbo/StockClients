from .earnings_releases import EarningsReleaseScraper
from .scraper import ZacksScraper
import warnings
from urllib3.exceptions import InsecureRequestWarning
import datetime
from dotenv import load_dotenv
import os

load_dotenv()

def setup_test():
    # Suprress warning from insecure request due to proxy use
    warnings.simplefilter("ignore", InsecureRequestWarning)

    # Instantiate the ZacksScraper object
    scraper = ZacksScraper(
        os.getenv("ZACKS_USER"), os.getenv("ZACKS_PASSWORD"), use_proxy=True
    )

    try:
        # Attempt to log in
        scraper.login()

        if scraper.logged_in:
            print("Successfully logged in.")
        else:
            print("Failed to log in.")
    except Exception as e:
        print(f"An error occurred: {e}")

    return scraper


def test_zacks_scraper():
    # Suprress warning from insecure request due to proxy use
    warnings.simplefilter("ignore", InsecureRequestWarning)

    # Instantiate the ZacksScraper object
    scraper = setup_test()

    try:
        # Attempt to log in
        scraper.login()

        if scraper.logged_in:
            print("Successfully logged in.")
        else:
            print("Failed to log in.")
    except Exception as e:
        print(f"An error occurred: {e}")

    scraper.run_stock_screen(
        [
            {
                "id": "zacks_rank",
                "value": "1",
                "operator": ">=",
            },
            {
                "id": "zacks_industry_rank",
                "value": "1",
                "operator": ">=",
            },
        ]
    )


def test_earnings_release():
    scraper = setup_test()

    er_scraper = EarningsReleaseScraper(scraper.session)
    df = er_scraper.scrape(datetime.datetime.now())
    print(df)


if __name__ == "__main__":
    # test_zacks_scraper()
    test_earnings_release()
