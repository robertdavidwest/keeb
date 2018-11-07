from pytz import timezone
from datetime import datetime
import json
from twilio.rest import Client
from sheets import get_gdrive_client, read_sheets
from run import get_keen_client
from run_bw_video_keen import get_keen_report


def get_twilio_client(path):
    d = json.load(open(path, 'r'))
    return Client(d['account_sid'], d['auth_token'])
    #message = client.messages.create(
    #        body="dogsBollocks", to="+18573000720", from_="+13392090657")


def get_alert_signals(gc):
    filters_title = "BW-Video-Keen-Key"
    signals = read_sheets(gc, filters_title, sheet="ALERT-SIGNALS")
    import ipdb; ipdb.set_trace()

    df_filter = df_filter.rename(columns={
        "FilterVariable": "property_name",
        "Formula": "operator",
        "Value": "property_value"})
    df_filter["operator"] = df_filter.operator.map(apply_operator_map)
    return df_filter.to_dict("records")


def main():
    keydir = "/home/robertdavidwest/"
    keen_client = get_keen_client(keydir +
        'keen-buzzworthy-aol.json')
    gdrive_client = get_gdrive_client(keydir +
         'gdrive-keen-buzzworthy-aol.json')
    twilio_client = get_twilio_client(keydir +
        'twilio.json')

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

    # Yesterday report
    report_name = "Today"
    timeframe = "this_day"
    sheetname = 'runtime: {} {} report: {}'.format(display_now, timezone_short, report_name)
    report = get_keen_report(keen_client, gdrive_client, timeframe, tz_str)
    signals = get_alert_signals(gdrive_client)
    alerts = check_for_alerts(report, signals)
    if alerts:
        send_sms(twilio_client, alerts)

if __name__ == '__main__':
    main()



