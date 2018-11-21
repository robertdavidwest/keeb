from copy import deepcopy
import json
import pandas as pd
import numpy as np
from twilio.rest import Client
import warnings
from time import time

import offline_sheets
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


def get_alert_rules(gc, offline=None):
    filters_title = "BW-Video-Keen-Key"
    sheet = "ALERT-RULES"
    if offline:
        return offline_sheets.read_sheets(filters_title, sheet)
    return read_sheets(gc, filters_title, sheet)


def get_alert_exclusions(gc, offline=None):
    filters_title = "BW-Video-Keen-Key"
    sheet = "ALERT-EXCLUSIONS"
    if offline:
        exclusions = offline_sheets.read_sheets(filters_title, sheet)
    else:
        exclusions = read_sheets(gc, filters_title, sheet)

    alertNames = exclusions.columns.tolist()
    alertNames.remove("campaign")
    for column in alertNames:
        exclusions['exclude-' + column] = exclusions[column] == 'x'
    exclusions = exclusions.drop(axis=1, labels=alertNames)
    return exclusions


def make_alert_msg(alertName, campaigns):
    return "'%s': check campaigns: '%s'" % (alertName, campaigns)


def _check_rule(data, rule):
    data = deepcopy(data)
    exclude_column = 'exclude-' + rule['alertName']
    idx = data[exclude_column] != True
    data = data[idx]

    check = pd.eval(rule['formula'])
    if check.any():
        campaigns = list(set(data[check].campaign))
        msg = make_alert_msg(rule['alertName'], campaigns)
        return msg


def apply_alert_rules(data, rules, exclusions):
    data = data.merge(exclusions, on='campaign', how='left', validate='m:1')
    alerts = [_check_rule(data, rule) for _, rule in rules.iterrows()]
    alerts = [x for x in alerts if x]
    warn_msg = "minimum preroll/playerload is: %s" % data['preroll/playerload'].min()
    warnings.warn(warn_msg)
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
    offline = False

    keydir = "/home/robertdavidwest/"
    #keydir = "/Users/rwest/"
    keen_client = get_keen_client(keydir +
        'keen-buzzworthy-aol.json')

    if offline:
        gdrive_client = None
    else:
        gdrive_client = get_gdrive_client(keydir +
             'gdrive-keen-buzzworthy-aol.json')
    twilio_client = get_twilio_client(keydir +
        'twilio.json')
    twilNumbers = get_twilio_numbers(keydir +
        'twilioNumbers.json')


    # Check previous 60 minutes
    tz_str = "US/Pacific"
    timeframe = "previous_60_minutes"
    rules = get_alert_rules(gdrive_client, offline)
    exclusions = get_alert_exclusions(gdrive_client, offline)
    report = get_keen_report(keen_client, gdrive_client, timeframe, tz_str,
            offline=offline)
    alerts = apply_alert_rules(report, rules, exclusions)
    idx = [c is None for c in report.campaign.tolist()]
    report.loc[idx, 'campaign'] = 'None'
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
