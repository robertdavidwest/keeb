from pytz import timezone
import pandas as pd
import numpy as np
from datetime import datetime
import offline_sheets
from sheets import get_gdrive_client, write_to_sheets, clean_sheets, read_sheets
from run import get_keen_client
from warnings import warn


def get_keen_table(client, timeframe, timezone, by, event, filters):
    result = client.count(event_collection=event,
                          timeframe=timeframe,
                          timezone=timezone,
                          filters=filters,
                          group_by=by)
    if not result:
        empty = {c: [] for c in by}
        empty.update({event: []})
        return pd.DataFrame(empty)
    else:
        return pd.DataFrame(result).rename(columns={'result': event})


def get_all_keen_data(client, timeframe, tz, filters=None, by=None):
    if not by:
        by = ['program', 'campaign', 'refer']
    events = ['pageviewevent', 'playerload', 'prerollplay', 'prerollend',
              'contentplay', 'cookiesdisabled', 'errorpage', 'halfevent',
              'rewardevent']
    datas = [get_keen_table(client, timeframe, tz, by, e, filters) for e in events]
    merge = lambda x, y: pd.merge(x, y,
                                  on=by,
                                  how='outer')
                                  #validate='1:1')

    data = reduce(merge, datas)

    data = data.sort_values(by)
    return data


def remove_dollar(x):
    if type(x) == str:
        if x == '':
            return np.nan
        return float(x.replace("$", ""))
    else:
        return x


def add_reference_rates(gc, data, offline=None):
    ref_rate_title = "BW-Video-Keen-Key"

    if offline:
        ref_rates = offline_sheets.read_sheets(ref_rate_title)
    else:
        ref_rates = read_sheets(gc, ref_rate_title)

    ref_rates = {k: df.replace("NULL", np.nan)
                 for k, df in ref_rates.iteritems()}
    ref_rates = {k: df.dropna(axis=0, how='all')
                 for k, df in ref_rates.iteritems()}


    rev_rates = ref_rates['REVENUE RATE']
    if rev_rates.revenue_rate.dtype == np.object:
        rev_rates['revenue_rate'] = rev_rates['revenue_rate'].apply(remove_dollar)

    cost_rates = ref_rates['COST RATE']
    if cost_rates.cost_rate.dtype == np.object:
        cost_rates['cost_rate'] = cost_rates['cost_rate'].apply(remove_dollar)

    data = data.merge(rev_rates,
                      on='program', how='left')

    data = data.merge(cost_rates,
                     on=['campaign', 'refer'],
                     how='left')
    return data


def add_encrave_costs(gc, data, report_type):
    title = "Encrave Report Summaries"
    encrave_costs = read_sheets(gc, title, report_type)
    encrave_costs = encrave_costs[['campaign', 'Cost', 'ReportName']]
    encrave_costs['refer'] = 'en'
    encrave_costs = encrave_costs.rename(
            columns={'Cost': 'encrave_cost',
                     'ReportName': "encrave_source"})
    data = data.merge(encrave_costs, on=['campaign', 'refer'],
            how='outer', validate='1:1', indicator=True)

    left_only_idx = (data.refer=='en') & (data._merge=='left_only')
    if left_only_idx.any():
        warn("keen data has 'en' campaigns not contained in Encrave report")
        warn("CampaignNames: %s" % data.loc[left_only_idx, 'campaign'].tolist())

    right_only_idx = (data.refer=='en') & (data._merge=='right_only')
    if right_only_idx.any():
        warn("Encrave data has campaigns not contained in keen data")
        warn("CampaignNames: %s" % data.loc[right_only_idx, 'campaign'].tolist())
    
    data = data[data._merge!='right_only']
    data.drop(axis=1, labels='_merge', inplace=True)
    return data


def apply_operator_map(key):
    operator_map = {
            "Equal to": "eq",
            "Not Equal to": "ne"}
    return operator_map.get(key, key)


def get_filters(gc, offline=None):
    filters_title = "BW-Video-Keen-Key"
    sheet = "FILTERS"
    if offline:
        df_filter = offline_sheets.read_sheets(filters_title, sheet)
    else:
        df_filter = read_sheets(gc, filters_title, sheet)

    df_filter = df_filter.rename(columns={
        "FilterVariable": "property_name",
        "Formula": "operator",
        "Value": "property_value"})
    df_filter["operator"] = df_filter.operator.map(apply_operator_map)
    return df_filter.to_dict("records")


def add_metrics(df):
    df['preroll/playerload'] = df['prerollplay']/df['playerload']
    df['preroll_complete_rate'] = df['prerollplay']/df['prerollend']
    df['preroll/content'] = df['prerollplay']/df['contentplay']
    df['error_rate'] = df['errorpage']/df['playerload']
    df['halfevent_rate'] = df['halfevent']/df['playerload']
    df['rewardevent_rate'] = df['rewardevent']/df['playerload']

    df['keen_rev'] = df['revenue_rate'] * df['prerollplay']

    notnull = df.cost_event_variable.notnull()
    cost_event_variables = list(set(
        df.loc[notnull, 'cost_event_variable']))
    df['keen_cost'] = np.nan
    for v in cost_event_variables:
        idx = df['cost_event_variable'] == v
        df.loc[idx, 'keen_cost'] = df.loc[idx, v] * df['cost_rate'] * df['cost_multiplier']

    df['keen_profit'] = df['keen_rev'] - df['keen_cost']
    df['keen_margin'] = df['keen_profit']/df['keen_cost']
    return df


def reorder_cols(df):
    final_order = [
         'program',
         'campaign',
         'refer',
         'pageviewevent',
         'playerload',
         'prerollplay',
         'preroll/playerload',
         'prerollend',
         'preroll_complete_rate',
         'contentplay',
         'preroll/content',
         'cookiesdisabled',
         'errorpage',
         'error_rate',
         'halfevent',
         'halfevent_rate',
         'rewardevent',
         'rewardevent_rate',
         'revenue_rate',
         'cost_rate',
         'cost_multiplier',
         'cost_event_variable',
         'keen_rev',
         'keen_cost',
         'encrave_cost',
         'encrave_source',
         'keen_profit',
         'keen_margin']
    if 'encrave_cost' not in df.columns.tolist():
        final_order.remove('encrave_cost')
    if 'encrave_source' not in df.columns.tolist():
        final_order.remove('encrave_source')

    #if set(df.columns) != set(final_order):
    #    print('extra cols: %s' % (set(df.columns) - set(final_order)))
    #    print('missing cols: %s' % (set(final_order) - set(df.columns)))
    #    raise AssertionError("cols not accounted for")
    return df[final_order]


def get_keen_report(kc, gc, timeframe, tz, enclave_report_type=None, by=None, offline=None):
    filters = get_filters(gc, offline)
    data = get_all_keen_data(kc, timeframe, tz, filters, by=by)
    data = add_reference_rates(gc, data, offline)
    data = add_metrics(data)
    data = data.drop_duplicates()
    if enclave_report_type:
        data = add_encrave_costs(gc, data, enclave_report_type)
    data = reorder_cols(data)
    return data


def main():
    title = 'BW-Video-Keen-Data-Snapshots'
    keen_client = get_keen_client(
        '/home/robertdavidwest/keen-buzzworthy-aol.json')
    gdrive_client = get_gdrive_client(
                 '/home/robertdavidwest/gdrive-keen-buzzworthy-aol.json')

    tz_str = "US/Pacific"
    timezone_short = "PT"
    tz = timezone(tz_str)

    this_now = datetime.now(tz)
    local_now =  datetime.now()
    pacific_now = datetime.now(
            timezone("US/Pacific")) # keen reports are pacific
    display_now = this_now.ctime()

    if this_now.day != local_now.day:
        raise AssertionError("Changing timezone " \
                "in sheetname will show incorrect day")
    
    # Yesterday report
    report_name = "Yesterday"
    timeframe = "previous_day"
    sheetname = 'runtime: {} {} report: {}'.format(display_now, timezone_short, report_name)
    report = get_keen_report(keen_client, gdrive_client, timeframe, tz_str, enclave_report_type=report_name)
    write_to_sheets(gdrive_client, report, title, sheetname)
    
    # Report Month to date excluding today
    report_name =  'MTD(not-today)'
    day = pacific_now.day
    n = day - 1
    if n == 0:
        timeframe = 'previous_month'
    else:
        timeframe = 'previous_{}_days'.format(n)
    sheetname = 'runtime: {} {} report: {}'.format(display_now, timezone_short, report_name)
    report = get_keen_report(keen_client, gdrive_client, timeframe, tz_str, enclave_report_type=report_name)
    write_to_sheets(gdrive_client, report, title, sheetname)

    # No more than 20 sheets in workbook. Older results are deleted.
    clean_sheets(gdrive_client, title, max_sheets=20)


if __name__ == '__main__':
    main()



