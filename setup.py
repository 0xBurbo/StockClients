from setuptools import setup, find_packages

setup(
    name="StockClients",
    version="0.2",
    description="Python modules to scrape stock data from various sources",
    author="Nate",
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=[
        # list of dependencies your package needs
        "numpy",
        "pandas",
        "bs4",
        "duckdb",
        "python-dotenv",
    ],
)
