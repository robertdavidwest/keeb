import keyring
from copy import deepcopy
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


def get_keen_data(client, timeframe, timezone, index):
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
                            group_by=index)
    prerolls = pd.DataFrame(prerolls)
    prerolls.rename(columns={'result': 'prerollplay'}, inplace=True)

    content = client.count(event_collection='contentplay',
                           timezone=timezone,
                           timeframe=timeframe,
                            group_by=index)
    content = pd.DataFrame(content)
    content.rename(columns={'result': 'contentplay'}, inplace=True)

    results = pd.merge(prerolls,
                       content,
                       on=index,
                       how='outer')
    results = results.sort_values(index, ascending=False)
    results.reset_index(inplace=True, drop=True)
    results.fillna(0, inplace=True)

    #if results.vidid.duplicated().any():
        #err_msg = 'Unable to aggregate data. keen reporting single video ids ' \
        #          'across multiple campaigns'
        #import ipdb; ipdb.set_trace()
        #raise AssertionError('keen reporting single video ids across multiple'
        #                     'campaigns')

    return results


def get_data(keen_client, gdrive_client, keen_timeframe, aol_timeframe):
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
            successful = True
        except Exception as e:
            print "AOL Data Grab attempt {} failed. Here is the exception message:".format(try_)
            print e

        try_ += 1

    if successful:
        keen_df = get_keen_data(keen_client, timeframe=keen_timeframe, timezone=timezone_str, index=['vidid', 'campaign'])
        keen_vendor_df = get_keen_data(keen_client, timeframe=keen_timeframe, timezone=timezone_str, index=['campaign', 'refer'])
        keen_vendor_df = keen_vendor_df.rename(columns={
            'campaign': 'Campaign',
            'refer': 'Referer',
            'prerollplay': 'Keen Preroll',
            'contentplay': 'Keen Content',
        })
        # make referer uppercase
        keen_vendor_df['Referer'] = keen_vendor_df['Referer'].str.upper()

        df = pd.merge(keen_df, aol_df,
                      on='vidid', how='outer', suffixes=('keen', 'aol'))

        df['Referer'] = 'All'
        df['AOL Performance'] = df['prerollplayaol']/df['contentplayaol']
        df['Keen Performance'] = df['prerollplaykeen']/df['contentplaykeen']

        df = df[[
            'vidid', 'Video title', 'campaign', 'Referer',
            'prerollplayaol', 'contentplayaol', 'AOL Performance',
            'prerollplaykeen', 'contentplaykeen', 'Keen Performance'
        ]]

        df = df.rename(columns={
            'vidid': 'Video ID',
            'Video title': 'Video Title',
            'campaign': 'Campaign',
            'prerollplayaol': 'AOL Preroll',
            'contentplayaol': 'AOL Content',
            'prerollplaykeen': 'Keen Preroll',
            'contentplaykeen': 'Keen Content'
        })

        # drop total row before aggregation up
        msk = df['Video Title'] != "Total"
        df = df[msk]
        df['Variance Preroll'] = df['AOL Preroll']/df['Keen Preroll']
        df['Variance Content'] = df['AOL Content']/df['Keen Content']


        df_campaign_sum = df.groupby('Campaign', as_index=False).agg({
            'AOL Preroll': 'sum',
            'AOL Content': 'sum',
            'Keen Preroll': 'sum',
            'Keen Content': 'sum'
        })

        df_campaign_sum['Referer'] = 'All'
        df_campaign_sum = df_campaign_sum.append(keen_vendor_df)
        df_campaign_sum['AOL Performance'] = df_campaign_sum['AOL Preroll']/df_campaign_sum['AOL Content']
        df_campaign_sum['Keen Performance'] = df_campaign_sum['Keen Preroll']/df_campaign_sum['Keen Content']

        df_campaign_sum = df_campaign_sum[[
            'Campaign', 'Referer',
            'AOL Preroll',
            'AOL Content',
            'AOL Performance',
            'Keen Preroll',
            'Keen Content',
            'Keen Performance'
        ]]

        df_campaign_sum['Variance Preroll'] = df_campaign_sum['AOL Preroll']/df_campaign_sum['Keen Preroll']
        df_campaign_sum['Variance Content'] = df_campaign_sum['AOL Content']/df_campaign_sum['Keen Content']

        df_campaign_sum = df_campaign_sum.fillna('-')
        df = df.fillna('-')

        return df_campaign_sum, df

        #create_compare_report(gdrive_client, [df_campaign_sum, df],
        #                      title, sheetname, blank_cols=[2, 0])
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
    '''
    # results from yesterday
    keen_timeframe = 'previous_1_days'
    aol_timeframe = 'Yesterday'
    df_campaign_sum_yest, df_details_yest = get_data(keen_client, gdrive_client, keen_timeframe, aol_timeframe)

    # results this month
    # results from this month
    keen_timeframe = 'this_month'
    aol_timeframe = eastern_now.strftime("%B")
    df_campaign_sum_mtd, df_details_mtd = get_data(keen_client, gdrive_client,  keen_timeframe, aol_timeframe)

    # merge results from different time frames
    df_campaign_sum = pd.merge(df_campaign_sum_mtd, df_campaign_sum_yest,
                               on=['Campaign', 'Referer'],
                               how='outer',
                               suffixes=(' MTD', ' YEST'))
    df_campaign_sum = df_campaign_sum.fillna('-')

    # in df_campaign_sum add blanks for missing referers to make data consistent
    # shape
    refer_cats = ['QR', 'TP', 'SB', 'O', 'SS', 'SS2']
    df_campaign_sum['Referer_cat'] = pd.Categorical(
        df_campaign_sum['Referer'],
        categories=refer_cats,
        ordered=True
    )
    df_campaign_sum = df_campaign_sum.sort_values(['Campaign', 'Referer_cat'])
    df_campaign_sum.drop(axis=1, labels='Referer_cat', inplace=True)
    '''
    refer_cats = ['QR', 'TP', 'SB', 'O', 'SS', 'SS2']
    df_campaign_sum = pd.read_csv('temp.csv')
    df_campaign_sum = df_campaign_sum.drop(axis=1, labels='Unnamed: 0')

    df_campaign_all_refs = pd.DataFrame()
    for _, row in df_campaign_sum.query("Referer != 'All'").iterrows():
        for cat in refer_cats:
            if row['Referer'] != cat:
                blank_row = deepcopy(row)
                blank_row[:] = '-'
                blank_row['Referer'] = cat
                blank_frame = pd.DataFrame(blank_row).transpose()
                df_campaign_all_refs = df_campaign_all_refs.append(blank_frame)

        row_df = pd.DataFrame(row).transpose()
        df_campaign_all_refs = df_campaign_all_refs.append(row_df)


    df_campaign_all_refs = df_campaign_sum.query("Referer == 'All'").append(df_campaign_all_refs)


    df_campaign_all_refs.to_csv('check.csv')
    '''
    df_details = pd.merge(df_details_mtd, df_details_yest,
                          on=['Video ID', 'Video Title', 'Campaign', 'Referer'],
                          how='outer',
                          suffixes=(' MTD', ' YEST'))
    df_details = df_details.fillna('-')
    '''
    # send to sheets
    #sheetname = 'keen/aol-{}'.format(display_now)
    sheetname = 'test'
    #create_compare_report(gdrive_client, [df_campaign_sum, df_details], title, sheetname, blank_cols=[2, 0])
    #create_compare_report(gdrive_client, [df_campaign_all_refs, df_campaign_all_refs], title, sheetname, blank_cols=[0, 0])

    # No more than 20 sheets in workbook. Older results are deleted.
    clean_sheets(gdrive_client, title, max_sheets=20)
