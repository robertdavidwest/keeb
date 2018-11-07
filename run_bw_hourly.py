from pytz import timezone
import pandas as pd
import numpy as np
from datetime import datetime

from sheets import get_gdrive_client, write_to_sheets, clean_sheets, read_sheets
from run import get_keen_client
from run_bw_video_keen import get_keen_report


def main():
    title = 'BW-Video-Keen-Hourly'
    keen_client = get_keen_client(
        '/home/robertdavidwest/keen-buzzworthy-aol.json')
    gdrive_client = get_gdrive_client(
                 '/home/robertdavidwest/gdrive-keen-buzzworthy-aol.json')

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
    write_to_sheets(gdrive_client, report, title, sheetname)

    clean_sheets(gdrive_client, title, max_sheets=1)

if __name__ == '__main__':
    main()



