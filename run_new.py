import pandas as pd
from run import get_keen_client


def get_keen_data(client, timeframe, on, event):
    result = client.count(event_collection=event, timeframe=timeframe, group_by=on)
    return pd.DataFrame(result)


def get_data_report(client, timeframe):
    on = ['campaign', 'refer']
    events = ['pageviewevent', 'playerload', 'prerollplay', 'prerollend',
              'contentplay', 'cookiesdisabled', 'errorpage', 'halfevent',
              'rewardevent']
    datas = [get_keen_data(client, timeframe, on, e) for e in events]
    merge = lambda x, y: pd.merge(x, y,
                                  on=on,
                                  how='outer',
                                  validate='1:1')
    data = reduce(datas, merge)
    return data


def main():
    timeframe = 'yesterday'
    keen_client = get_keen_client(
        '/home/robertdavidwest/keen-buzzworthy-aol.json')

    report = get_data_report(keen_client, timeframe)
    return report


if __name__ == '__main__':
    result = main()


