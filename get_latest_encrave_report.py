from datetime import datetime, timedelta
from google_api import gmail
from sheets import (get_gdrive_client, 
                    read_sheets, 
                    write_to_sheets, 
                    delete_sheet)

GMAIL_CREDENTIALS_PATH = "/home/robertdavidwest/databreakthroughs-gmail-credentials.json"
GMAIL_TOKEN_PATH = "/home/robertdavidwest/databreakthroughs-gmail-token.json"


def get_encrave_report(date_yesterday):
    email_subject =  'Encrave: buzzworthy MTD as of %s' % date_yesterday
    service = gmail.get_gmail_service(GMAIL_CREDENTIALS_PATH, GMAIL_TOKEN_PATH)
    csvs = gmail.query_for_csv_attachments(service, email_subject)
    if len(csvs) != 1: 
        raise AssertionError("more than one csv, check query")
    data = csvs[0]['data']
    name = csvs[0]['emailsubject']
    data = data.rename(columns={'Campaign Name': 'campaign'})
    data['campaign'] = data.campaign.str.lower()
    return data, name


def aggregate(data, date_yesterday, report_name):
    mtd = data.groupby("campaign", as_index=False).agg({'Cost': 'sum'})
    mtd['ReportName'] = report_name
    date_yesterday = date_yesterday.replace("-","/")
    yesterday = data.query("Date == @date_yesterday")
    yesterday['ReportName'] = report_name
    return mtd, yesterday 


def write_encrave_report(gc, data, sheet):
    title = "Encrave Report Summaries"
    delete_sheet(gc, title, sheet)
    write_to_sheets(gc, data, title, sheet)


def main():
    keydir = "/home/robertdavidwest/"
    gc = get_gdrive_client(keydir +
             'gdrive-keen-buzzworthy-aol.json')
    date_yesterday = datetime.strftime(datetime.now() - timedelta(1), '%m-%d-%Y')
    data, report_name  = get_encrave_report(date_yesterday)
    mtd_report, yest_report = aggregate(data, date_yesterday, report_name)
    write_encrave_report(gc, mtd_report, "MTD(not-today)")
    write_encrave_report(gc, yest_report, "Yesterday")


if __name__ == '__main__':
    main()
