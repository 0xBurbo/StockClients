from abc import ABC, abstractmethod
from typing import List, Dict, Any, OrderedDict


class QueryStrategy(ABC):
    @abstractmethod
    def write_query(self, value, operator):
        pass


class OperatorAQuery(QueryStrategy):
    def __init__(self, p_items, p_item_name, p_item_key) -> None:
        super().__init__()
        self.p_items = p_items
        self.p_item_name = p_item_name
        self.p_item_key = p_item_key

    def write_query(self, value, operator):
        operatorAMap = {
            ">=": 6,
            "<=": 7,
            "=": 8,
            "<>": 17,
        }

        return OrderedDict(
            [
                ("operator[]", operatorAMap[operator]),
                ("value[]", value),
                ("p_items[]", self.p_items),
                ("p_item_name[]", self.p_item_name),
                ("p_item_key[]", self.p_item_key),
            ]
        )


class OperatorBQuery(QueryStrategy):
    def __init__(self, p_items, p_item_name, p_item_key) -> None:
        super().__init__()
        self.p_items = p_items
        self.p_item_name = p_item_name
        self.p_item_key = p_item_key

    def write_query(self, value, operator):
        operatorBMap = {
            ">=": 12,
            "<=": 13,
            "=": 19,
            "<>": 20,
        }

        return OrderedDict(
            [
                ("operator[]", operatorBMap[operator]),
                ("value[]", value),
                ("p_items[]", self.p_items),
                ("p_item_name[]", self.p_item_name),
                ("p_item_key[]", self.p_item_key),
            ]
        )


# ... define more strategies here ...

strategies = {
    "zacks_rank": OperatorAQuery("15005", "Zacks Rank", "0"),
    "zacks_industry_rank": OperatorAQuery("15025", "Zacks Industry Rank", "1"),
    "value_score": OperatorBQuery("15030", "Value Score", "2"),
    "growth_score": OperatorBQuery("15035", "Growth Score", "3"),
    "momentum_score": OperatorBQuery("15040", "Momentum Score", "4"),
    "vgm_score": OperatorBQuery("15045", "VGM Score", "5"),
    "earnings_esp": OperatorAQuery("17060", "Earnings ESP", "6"),
    "52_week_high": OperatorAQuery("14010", "52 Week High", "7"),
    "market_cap": OperatorAQuery("12010", "Market Cap (mil)", "8"),
    "last_eps_surprise": OperatorAQuery("17005", "Last EPS Surprise (%)", "9"),
    "p/e_f1": OperatorAQuery("22010", "P/E (F1)", "10"),
    "num_brokers": OperatorAQuery("16010", "# of Brokers in Rating", "11"),
    "optionable": [], # TODO
    "percent_change_f1": OperatorAQuery("18020", "% Change F1 Est. (4 weeks)", "13"),
    "div_yield": OperatorAQuery("25005", "Div. Yield %", "14"),
    "avg_volume": OperatorAQuery("12015", "Avg. Volume", "15"),
    "last_reported_qtr": OperatorAQuery("17030", "Last Reported Qtr (yyyymm)", "68"),
    "last_eps_report_date": OperatorAQuery("17050", "Last EPS Report Date (yyyymmdd)", "72"),
    "next_eps_report_date": OperatorAQuery("17055", "Next EPS Report Date (yyyymmdd)", "73"),
    "q0_consensus_est": OperatorAQuery("19005", "Q0 Consensus Est. (last completed fiscal Qtr)", "80"),
    # ... add more strategies here ...
}


def write_query(config: List[Dict[str, Any]]):
    # Common form data for all requests
    form_data = [
        ("is_only_matches", "1"),
        ("is_premium_exists", "0"),
        ("is_edit_view", "0"),
        ("saved_screen_name", ""),
        ("tab_id", "1"),
        ("start_page", "1"),
        ("no_of_rec", "15"),
        ("sort_col", "2"),
        ("sort_type", "ASC"),
    ]

    for item in config:
        id = item.get("id")
        value = item.get("value")
        operator = item.get("operator")

        strategy = strategies.get(id)
        if strategy:
            parameters = strategy.write_query(value, operator)
            for key in parameters:
                form_data.append((key, parameters[key]))
        else:
            print(f"Unknown id: {id}")

    return form_data
