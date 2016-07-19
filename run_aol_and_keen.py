import keyring
import json
import pandas as pd
from datetime import datetime
from pytz import timezone

from keen.client import KeenClient
from selenium_aol import get_aol_data
import selenium
from sheets import get_gdrive_client, clean_sheets, create_compare_report


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


def get_keen_data(client, timeframe, timezone):
    """
    Count both 'prerollplay' and 'contentplay' from keen, aggregated by
    campaign and refer

    Parameters
    ----------
    client : keen.client.KeenClient
    timeframe : str
        time frame to pull metric from keen. e.g. 'this_1_day',
        'previous_7_days'

         see all keen timeframe options here:
         https://keen.io/docs/api/#timeframe
    timezone : int
        the timezone you'd like to use for the timeframe
        and interval in seconds

    Returns
    -------
    pd.DataFrame
        a dataframe of results:
        | campaign | refer | prerollplay | contentplay |
    """
    print 'reading: {}'.format(timeframe)

    prerolls = client.count(event_collection='prerollplay',
                            timezone=timezone,
                            timeframe=timeframe,
                            group_by=['vidid', 'campaign'])
    prerolls = pd.DataFrame(prerolls)
    prerolls.rename(columns={'result': 'prerollplay'}, inplace=True)

    content = client.count(event_collection='contentplay',
                           timezone=timezone,
                           timeframe=timeframe,
                            group_by=['vidid', 'campaign'])
    content = pd.DataFrame(content)
    content.rename(columns={'result': 'contentplay'}, inplace=True)

    results = pd.merge(prerolls,
                       content,
                       on=['vidid', 'campaign'],
                       how='outer')
    results = results.sort_values(['vidid'], ascending=False)
    results.reset_index(inplace=True, drop=True)
    results.fillna(0, inplace=True)

    #if results.vidid.duplicated().any():
        #err_msg = 'Unable to aggregate data. keen reporting single video ids ' \
        #          'across multiple campaigns'
        #import ipdb; ipdb.set_trace()
        #raise AssertionError('keen reporting single video ids across multiple'
        #                     'campaigns')

    return results


def get_data_and_report(keen_client, gdrive_client, keen_timeframe, aol_timeframe, sheetname, title):
    """
    Get data from keen api and from AOL then merge the datasets
    and right the results to googlesheets

    keen_client:
        return value of get_keen_client()
    gdrive_client
        return value of get_gdrive_client()
    keen_timeframe : str
        represents timeframe to agg data over
    aol_timeframe : str
        represents timeframe to agg data over
    sheetname: str
        the sheetname to write to
    title
        the title of the google doc to write to
`
    """
    # get aol data
    # try 10 times, if not successful then print an err message to google sheets
    successful = False
    max_trys = 10
    try_ = 1
    while ((not successful) & (try_ < max_trys)) :
        try:
            aol_df = get_aol_data(aol_credentials['username'], aol_credentials['password'], aol_timeframe, "firefox")
        except Exception as e:
            print "AOL Data Grab attempt {} failed. Here is the exception message:".format(try_)
            print e
            aol_df = get_aol_data(aol_credentials['username'], aol_credentials['password'], aol_timeframe, "firefox")
        finally:
            successful = True

        try_ += 1

    if successful:
        keen_df = get_keen_data(keen_client, timeframe=keen_timeframe, timezone=timezone_str)

        df = pd.merge(keen_df, aol_df,
                      on='vidid', how='outer', suffixes=('keen', 'aol'))

        df = df[[
            'vidid', 'Video title', 'campaign',
            'prerollplayaol', 'prerollplaykeen',
            'contentplayaol', 'contentplaykeen'
        ]]

        df = df.rename(columns={
            'vidid': 'Video ID',
            'Video title': 'Video Title',
            'campaign': 'Campaign',
            'prerollplayaol': 'AOL Preroll Play',
            'prerollplaykeen': 'Keen Preroll Play',
            'contentplayaol': 'AOL Content Play',
            'contentplaykeen': 'Keen Content Play'
        })

        # drop total row before aggregation up
        msk = df['Video Title'] != "Total"
        df = df[msk]

        df_campaign_sum = df.groupby('Campaign', as_index=False).agg({
            'AOL Preroll Play': 'sum',
            'Keen Preroll Play': 'sum',
            'AOL Content Play': 'sum',
            'Keen Content Play': 'sum'
        })

        df_campaign_sum = df_campaign_sum[[
            'Campaign',
            'AOL Preroll Play',
            'Keen Preroll Play',
            'AOL Content Play',
            'Keen Content Play'
        ]]

        df_campaign_sum = df_campaign_sum.fillna('-')
        df = df.fillna('-')

        create_compare_report(gdrive_client, [df_campaign_sum, df],
                              title, sheetname, blank_cols=[2, 0])
    else:
        err_msg = 'Error Scraping AOL Platform. Please try again later'

        df = pd.DataFrame({
            'ErrReport': [err_msg]

        })
        create_compare_report(gdrive_client, [df],
                              title, sheetname, blank_cols=[0])


if __name__ == '__main__':

    # Both the AOL Portal and the keen api are pulling data by the time frame
    # specified in eastern time

    timezone_str = 'US/Eastern'
    tz = timezone(timezone_str )
    eastern_now = datetime.now(tz)
    local_now =  datetime.now()
    display_now = eastern_now.ctime()

    if eastern_now.day != local_now.day:
        raise AssertionError("Changing to Eastern time in sheetname will show"
                             "incorrect day")

    title = "Buzzworthy - Keen - AOL - Compare Plays"

    # ALl needed credentials:

    aol_portal_json = '/home/robertdavidwest/aol-portal.json'
    aol_credentials = json.load(open(aol_portal_json, 'r'))

    keen_client = get_keen_client(
        '/home/robertdavidwest/keen-buzzworthy-aol.json')

    gdrive_client = get_gdrive_client(
        '/home/robertdavidwest/gdrive-keen-buzzworthy-aol.json')

    # results from yesterday
    keen_timeframe = 'previous_1_days'
    aol_timeframe = 'Yesterday'
    sheetname = 'keen/aol-{} {}'.format(display_now, aol_timeframe)
    get_data_and_report(keen_client, gdrive_client, keen_timeframe, aol_timeframe, sheetname, title)

    # results this month
    # results from this month
    keen_timeframe = 'this_month'
    aol_timeframe = eastern_now.strftime("%B")

    sheetname = 'keen/aol-{} {}'.format(display_now, aol_timeframe)
    get_data_and_report(keen_client, gdrive_client,  keen_timeframe, aol_timeframe, sheetname, title)

    # No more than 20 sheets in workbook. Older results are deleted.
    clean_sheets(gdrive_client, title, max_sheets=20)
