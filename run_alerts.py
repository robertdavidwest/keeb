from pytz import timezone
from datetime import datetime
import json
import pandas as pd
from twilio.rest import Client
from sheets import get_gdrive_client, read_sheets
from run import get_keen_client
from run_bw_video_keen import get_keen_report


def get_twilio_client(path):
    d = json.load(open(path, 'r'))
    return Client(d['account_sid'], d['auth_token'])


def get_twilio_numbers(path):
    return json.load(open(path, 'r'))


def send_sms(client, msg, twilio_numbers):
    from_ = twilio_numbers['from'].replace("-","").replace(" ", "")
    for to in twilio_numbers['to']:
        print("sending alert msg {} to {}". format(msg, to))
        client.messages.create(
                body=msg,
                to=to.replace("-","").replace(" ", ""),
                from_=from_)


def get_alert_rules(gc):
    filters_title = "BW-Video-Keen-Key"
    return read_sheets(gc, filters_title, sheet="ALERT-RULES")


def make_alert_msg(alertName, campaigns):
    return "'%s': check campaigns: '%s'" % (alertName, campaigns)


def apply_alert_rules(data, rules):
    alerts = []
    for i, row in rules.iterrows():
        check = pd.eval(row['formula'])
        if check.any():
            campaigns = list(set(data[check].campaign))
            msg = make_alert_msg(row['alertName'], campaigns)
            alerts.append(msg)
    return alerts


def main():
    keydir = "/home/robertdavidwest/"
    keen_client = get_keen_client(keydir +
        'keen-buzzworthy-aol.json')
    gdrive_client = get_gdrive_client(keydir +
         'gdrive-keen-buzzworthy-aol.json')
    twilio_client = get_twilio_client(keydir +
        'twilio.json')
    twilNumbers = get_twilio_numbers(keydir +
        'twilioNumbers.json')

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

    # Today report
    timeframe = "previous_60_minutes"
    rules = get_alert_rules(gdrive_client)
    report = get_keen_report(keen_client, gdrive_client, timeframe, tz_str)
    alerts = apply_alert_rules(report, rules)
    for a in alerts:
        send_sms(twilio_client, a, twilNumbers)


if __name__ == '__main__':
    main()



