__author__ = 'rwest'

import json
import keyring
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def get_gdrive_client(keyring_service_name):
    scope = ['https://spreadsheets.google.com/feeds']

    credentionals_json = keyring.get_password(keyring_service_name,
                                              'credentionals_json')
    credentials = json.loads(credentionals_json)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials,
                                                                   scope)
    gc = gspread.authorize(credentials)
    return gc


def write_to_sheets(gc, data, title, sheetname):
    """
    Add a timestamp in the first row then:

    Write the data in "data" to a google sheet named "title" on a new sheet
    "sheetname". If "sheetname" exists an error will be thrown.

    Parameters
    ----------
    gc : gspread.authorize
        google drive client
    data : pd.DataFrame
    title : str
        sheets title
    sheetname : str
        the sheetname in the google sheet
    """
    print 'writing to sheet: {}'.format(sheetname)

    wb = gc.open(title)
    wb.add_worksheet(title=sheetname, rows=1, cols=1)
    wks = wb.worksheet(sheetname)

    # add data
    for i, row in data.iterrows():
        wks.insert_row(values=row.tolist(), index=1)

    # add header row
    wks.insert_row(data.columns.tolist(), index=1)
