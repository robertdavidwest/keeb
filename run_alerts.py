from pytz import timezone
from datetime import datetime
import json
import pandas as pd
import numpy as np
from twilio.rest import Client
import warnings
from time import time

from sheets import get_gdrive_client, read_sheets
from run import get_keen_client
from run_bw_video_keen import get_keen_report


KEEP_ALERT_TIME = 60*60 # seconds


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

    msg = "minimum preroll/playerload is: %s" % data['preroll/playerload'].min()
    warnings.warn(msg)
    return alerts


def check_alert_log(alerts, alert_log):
    t = time()
    if alert_log is None:
        alert_log = pd.DataFrame({"msg": [], "time": []})

    alerts = pd.DataFrame([{"msg": a} for a in alerts])
    alerts['send'] = True
    alerts['time'] = t

    alert_check = pd.merge(
        alert_log, 
        alerts, 
        on='msg', 
        how='outer',
        suffixes=['Prev', 'Now'])
    
    alert_check['time_passed'] = alert_check['timeNow'] - alert_check['timePrev']
    idx = alert_check['time_passed'] < KEEP_ALERT_TIME
    alert_check.loc[idx, 'send'] = False
    alert_check["time"] = np.where(alert_check["send"], 
        alert_check["timeNow"], alert_check["timePrev"])
    idx2 = alert_check['send']==True
    alerts_to_send = alert_check.loc[idx2, 'msg'].tolist()
    alert_log = alert_check[["msg", "time"]]
    return alerts_to_send, alert_log


def main(alert_log=None):
    keydir = "/home/robertdavidwest/"
    keen_client = get_keen_client(keydir +
        'keen-buzzworthy-aol.json')
    gdrive_client = get_gdrive_client(keydir +
         'gdrive-keen-buzzworthy-aol.json')
    twilio_client = get_twilio_client(keydir +
        'twilio.json')
    twilNumbers = get_twilio_numbers(keydir +
        'twilioNumbers.json')

    # Check previous 60 minutes
    tz_str = "US/Pacific"
    timeframe = "previous_60_minutes"
    rules = get_alert_rules(gdrive_client)
    report = get_keen_report(keen_client, gdrive_client, timeframe, tz_str)
    alerts = apply_alert_rules(report, rules)

    if alerts:
        alerts_to_send, alert_log = check_alert_log(alerts, alert_log)
    else:
        print("no alerts")
        alerts_to_send = []

    for a in alerts_to_send:
        send_sms(twilio_client, a, twilNumbers)

    return alert_log


if __name__ == '__main__':
    main()


