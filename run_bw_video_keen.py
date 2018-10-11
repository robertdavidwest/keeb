from pytz import timezone
import pandas as pd
import numpy as np
from datetime import datetime
from sheets import get_gdrive_client, write_to_sheets, clean_sheets, read_sheets
from run import get_keen_client, write_to_sheets


def get_keen_table(client, timeframe, on, event):
    result = client.count(event_collection=event, timeframe=timeframe, group_by=on)
    return pd.DataFrame(result).rename(columns={'result': event})


def get_all_keen_data(client, timeframe):
    on = ['program', 'campaign', 'refer']
    events = ['pageviewevent', 'playerload', 'prerollplay', 'prerollend',
              'contentplay', 'cookiesdisabled', 'errorpage', 'halfevent',
              'rewardevent']
    datas = [get_keen_table(client, timeframe, on, e) for e in events]
    merge = lambda x, y: pd.merge(x, y,
                                  on=on,
                                  how='outer')
                                  #validate='1:1')

    data = reduce(merge, datas)

    data = data.sort_values(['refer', 'campaign'])
    return data


def add_reference_rates(gc, data):
    ref_rate_title = "BW-Video-Keen-Key"
    ref_rates = read_sheets(gc, ref_rate_title)
    ref_rates = {k: df.replace("NULL", np.nan)
                 for k, df in ref_rates.iteritems()}
        
    data = data.merge(ref_rates['REVENUE RATE'],
                      on='program', how='left')

    data = data.merge(ref_rates['COST RATE'],
                     on=['campaign', 'refer'], 
                     how='left')
    return data


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
        df.loc[idx, 'keen_cost'] = df.loc[idx, v] * df['cost_rate']

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
         'cost_event_variable',
         'keen_rev',
         'keen_cost',
         'keen_profit',
         'keen_margin']
    if set(df.columns) != set(final_order):
        raise AssertionError("cols not accounted for")
    return df[final_order]


def get_keen_report(kc, gc, timeframe):
    data = get_all_keen_data(kc, timeframe)
    data = add_reference_rates(gc, data)
    data = add_metrics(data)
    data = reorder_cols(data)
    return data


def main():
    title = 'BW-Video-Keen-Data-Snapshots'
    keen_client = get_keen_client(
        '/home/robertdavidwest/keen-buzzworthy-aol.json')
    gdrive_client = get_gdrive_client(
                 '/home/robertdavidwest/gdrive-keen-buzzworthy-aol.json')
    
    thistimezone = "US/Eastern"
    timezone_short = "EST"
    tz = timezone('US/Eastern')

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
    report = get_keen_report(keen_client, gdrive_client, timeframe) 
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
    report = get_keen_report(keen_client, gdrive_client, timeframe) 
    write_to_sheets(gdrive_client, report, title, sheetname)

    # No more than 20 sheets in workbook. Older results are deleted.
    clean_sheets(gdrive_client, title, max_sheets=20)


if __name__ == '__main__':
    main()



