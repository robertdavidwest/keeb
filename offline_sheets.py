import pandas as pd


def get_alert_rules_sample():
    return pd.DataFrame([{'alertName': 'broken-player-alert',
                          'formula': '(data["playerload"] > 1000)'},
                          {'alertName': 'no-fill-alert',
                           'formula': 'data["preroll/playerload"] < 0.2'}
                         ])


def get_alert_exclusions_sample():
    return pd.DataFrame([
        {'campaign': "None", "no-fill-alert": "x"},
        {'campaign': "stories-editorial-androidonly", "no-fill-alert": "x"},
        {'campaign': "example-campaign", "no-fill-alert": ""},
        {'campaign': "dogs", "broken-player-alert": "x"}])


def get_filters_sample():
    return pd.DataFrame([{
            'FilterVariable': 'playerid',
            'Formula': "Not Equal to",
            'Value': "35c29464-50f0-4e82-a1db-a7a328d24c58"}])


def get_cost_rate_sample():
    return pd.DataFrame([{
        'campaign': 'eeditorial-hearst-runnerutm_keyword=notset',
        'refer': 'irsr',
        'cost_rate': 0.035,
        'cost_multiplier': 1,
        'cost_event_variable': 'rewardevent'}])


def get_revenue_rate_sample():
    return pd.DataFrame([{'program': 'aol', 'revenue_rate': 0.012}])


sheet_to_function_map = {
    "ALERT-RULES": get_alert_rules_sample(),
    "ALERT-EXCLUSIONS": get_alert_exclusions_sample(),
    "FILTERS": get_filters_sample(),
    "COST RATE": get_cost_rate_sample(),
    "REVENUE RATE": get_revenue_rate_sample()}


def get_sheet_names():
    return ["ALERT-RULES", "ALERT-EXCLUSIONS", "FILTERS",
            "COST_RATE", "REVENUE_RATE"]

def read_sheets(title, sheet=None):
    if title != 'BW-Video-Keen-Key':
        raise AssertionError("not implemented")
    if sheet:
        return sheet_to_function_map[sheet]
    else:
        return sheet_to_function_map
