from datetime import datetime, timedelta
from google_api import gmail
from sheets import (get_gdrive_client, 
                    read_sheets, 
                    write_to_sheets, 
                    delete_sheet)

GMAIL_CREDENTIALS_PATH = "/home/robertdavidwest/databreakthroughs-gmail-credentials.json"
GMAIL_TOKEN_PATH = "/home/robertdavidwest/databreakthroughs-gmail-token.json"


def get_encrave_report():
    yesterday = datetime.strftime(datetime.now() - timedelta(1), '%m-%d-%Y')
    email_subject =  'Encrave: buzzworthy MTD as of %s' % yesterday
    service = gmail.get_gmail_service(GMAIL_CREDENTIALS_PATH, GMAIL_TOKEN_PATH)
    csvs = gmail.query_for_csv_attachments(service, email_subject)
    if len(csvs) != 1: 
        raise AssertionError("more than one csv, check query")
    data = csvs[0]['data']
    name = csvs[0]['emailsubject']
    data = data.rename(columns={"Client": name})
    return data.head()


def write_encrave_report(gc, data):
    title = "BW-Video-Keen-Key"
    sheet = "ENCRAVE-REPORT"
    delete_sheet(gc, title, sheet)
    write_to_sheets(gc, data, title, sheet)


def main():
    keydir = "/home/robertdavidwest/"
    gc = get_gdrive_client(keydir +
             'gdrive-keen-buzzworthy-aol.json')
    data = get_encrave_report()
    write_encrave_report(gc, data)


if __name__ == '__main__':
    main()
