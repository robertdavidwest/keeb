import keyring
import json
import pandas as pd
from datetime import datetime
from pytz import timezone

from keen.client import KeenClient

from sheets import get_gdrive_client, write_to_sheets, clean_sheets


def get_keen_client(credentials_key):
    """ Get KeenCLient

    Parameters
    ----------
    credentials_key : str
        either a path to a json file containing 'project_id' and 'read_key'
        or a service_name for keyring entries containing 'project_id' and
        'read_key'

    Returns
    -------
    KeenClient
    """
    if credentials_key.endswith('.json'):
        credentials = open(credentials_key, 'r').read()
        credentials = json.loads(credentials)
        project_id = credentials['project_id']
        read_key = credentials['read_key']
    else:
        project_id=keyring.get_password(credentials_key, 'project_id')
        read_key=keyring.get_password(credentials_key, 'read_key')

    client = KeenClient(project_id=project_id, read_key=read_key)
    return client


def get_keen_data(client, timeframe):
    """
    Count both 'prerollplay' and 'contentplay' from keen, aggregated by
    campaign and refer
im
    Parameters
    ----------
    client : keen.client.KeenClient
    time_frame : str
        time frame to pull metric from keen. e.g. 'this_1_day',
        'previous_7_days'

    Returns
    -------
    pd.DataFrame
        a dataframe of results:
        | campaign | refer | prerollplay | contentplay |
    """
    print 'reading: {}'.format(timeframe)

    prerolls = client.count(event_collection='prerollplay',
                            timeframe=timeframe,
                            group_by=['campaign', 'refer'])
    prerolls = pd.DataFrame(prerolls)
    prerolls.rename(columns={'result': 'prerollplay'}, inplace=True)

    content = client.count(event_collection='contentplay',
                            timeframe=timeframe,
                            group_by=['campaign', 'refer'])
    content = pd.DataFrame(content)
    content.rename(columns={'result': 'contentplay'}, inplace=True)

    results = pd.merge(prerolls,
                       content,
                       on=['campaign', 'refer'],
                       how='outer')
    results = results.sort_values(['campaign', 'refer'], ascending=False)
    results.reset_index(inplace=True, drop=True)
    results.fillna(0, inplace=True)
    return results


if __name__ == '__main__':

    credentials_switch = 'json'

    title = "Buzzworthy - Keen - AOL - Datafeed"

    if credentials_switch == 'keyring':
        keen_client = get_keen_client('keen-buzzworthy-aol')
        gdrive_client = get_gdrive_client('gdrive-keen-buzzworthy-aol')

    elif credentials_switch == 'json':
        keen_client = get_keen_client(
            'home/robertdavidwest/keen-buzzworthy-aol.json')

        gdrive_client = get_gdrive_client(
            'home/robertdavidwest/gdrive-keen-buzzworthy-aol.json')
    else:
        exit()

    tz = timezone('US/Eastern')
    eastern_now = datetime.now(tz)
    local_now =  datetime.now()
    display_now = eastern_now.ctime()

    if eastern_now.day != local_now.day:
        raise AssertionError("Changing to Eastern time in sheetname will show"
                             "incorrect day")

    # see all timeframe options here:
    # https://keen.io/docs/api/#timeframe

    timeframe = 'previous_1_days'
    sheetname = '{} {}'.format(display_now, 'Yesterday')
    results = get_keen_data(keen_client, timeframe=timeframe)
    write_to_sheets(gdrive_client, results, title, sheetname)

    timeframe = 'this_month'
    sheetname = '{} {}'.format(display_now, timeframe)
    results = get_keen_data(keen_client, timeframe=timeframe)
    write_to_sheets(gdrive_client, results, title, sheetname)

    # No more than 20 sheets in workbook. Older results are deleted.
    clean_sheets(gdrive_client, title, max_sheets=20)