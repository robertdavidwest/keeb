__author__ = 'rwest'

import pandas as pd
import numpy as np
import json
import keyring
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def get_gdrive_client(credentials_key):
    """ Get gspread client

    Parameters
    ----------
    credentials_key : str
        either a path to a json file containing 'project_id' and 'read_key'
        or a service_name for keyring entries containing 'project_id' and
        'read_key'

    Returns
    -------
    gspread client
    """
    if credentials_key.endswith('.json'):
        credentionals_json = open(credentials_key, 'r').read()
    else:
        credentionals_json = keyring.get_password(credentials_key,
                                                 'credentionals_json')

    scope = ['https://spreadsheets.google.com/feeds']

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


def read_sheets(gc, title, sheet=None):
    sheets = gc.open(title).worksheets()
    if sheet:
        result = [pd.DataFrame(s.get_all_records())
                for s in sheets if s.title == sheet]
        if len(result) > 0:
            return result[0]
        else:
            return None

    return {s.title: pd.DataFrame(s.get_all_records())
           for s in sheets}


def write_df(wks, df, row, col):
    """Write a pd.DataFrame to a google sheets sheet

    Parameters
    ----------
    wks : gspread worksheet
        a gspreadworksheet
    df : pd.DataFrame
    row : int
        row to put header of df (indexed from 0)
    col : int
        col to put first row of df (indexed from 0)
    """

    # header row
    for j, col_name in enumerate(df.columns):
        if 'AOL' in col_name:
            wks.update_cell(row+1, col+j+1, 'AOL')
            wks.update_cell(row+2, col+j+1, col_name.replace('AOL', ''))
        elif 'Keen' in col_name:
            wks.update_cell(row+1, col+j+1, 'Keen')
            wks.update_cell(row+2, col+j+1, col_name.replace('Keen', ''))
        else:
            wks.update_cell(row+2, col+j+1, col_name)
    # data
    for i, row_of_data in df.iterrows():
        for j, col_name in enumerate(df.columns):
            wks.update_cell(row+3+i, col+j+1, row_of_data[j])


def create_compare_report(gc, data, title, sheetname, blank_cols=None):

    cols = max(len(data[0].columns), len(data[1].columns))
    rows = len(data[0]) + len(data[1]) + 3 + 2 # 2 additonal rows to split out the header columns

    wb = gc.open(title)
    wb.add_worksheet(title=sheetname, rows=rows, cols=cols)
    wks = wb.worksheet(sheetname)

    current_row = 0
    for i, d in enumerate(data):

        if blank_cols:
            col = blank_cols[i]
        else:
            col = 0

        write_df(wks, d, row=current_row, col=col)
        current_row += len(d) + 2 + 1


def clean_sheets(gc, title, max_sheets):
    """
    Keep the number of sheets in the workbook to a maximum of 'max_sheets'.
    This method assumes the sheetname contains a time stamp in the first
    24 chars and will remove worksheets by ages until there are at most '
    max_sheets' If the sheet does not contain such a time stamp then it will
    not be removed

    Parameters
    ----------
    gc : gspread.authorize
        google drive client
    title : str
        sheets title
    max_sheets : str
        the maximum number of sheets to keep in the workbook
    """
    wb = gc.open(title)
    worksheets = wb.worksheets()

    if len(worksheets) <= max_sheets:
        return

    names = [w.title for w in worksheets]
    dates = []
    for name in names:
        try:
            date = pd.to_datetime(name[:24])
            dates.append(date)
        except ValueError:
            dates.append(np.nan)

    sheet_df = pd.DataFrame({'date': dates}, index=names).sort_values('date')
    num_drops = len(sheet_df) - max_sheets
    drop_sheet_df = sheet_df[:num_drops]

    print 'Sheet limit reached. {} sheets will be deleted'.format(
        len(drop_sheet_df))

    for ws in worksheets:
        if ws.title in drop_sheet_df.index:
            print 'deleting sheet: ' + ws.title
            wb.del_worksheet(ws)
