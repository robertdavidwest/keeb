from sheets import get_gdrive_client, read_sheets, write_to_sheets, delete_sheet
import offline_sheets
from run_bw_video_keen import get_keen_client, get_all_keen_data


from run_alerts import get_alert_exclusions
def get_campaigns(client, timeframe, tz):
    by = ['campaign']
    data = get_all_keen_data(client, timeframe, tz, by=by)
    campaigns = data[['campaign']]
    return campaigns


def get_alert_exclusions(gc, offline=None):
    filters_title = "BW-Video-Keen-Key"
    sheet = "ALERT-EXCLUSIONS"
    if offline:
        exclusions = offline_sheets.read_sheets(filters_title, sheet)
    else:
        exclusions = read_sheets(gc, filters_title, sheet)
    return exclusions


def update_exclusions_in_sheets(gc, exclusions):
    title = "BW-Video-Keen-Key"
    sheet = "ALERT-EXCLUSIONS"
    delete_sheet(gc, title, sheet)
    write_to_sheets(gc, exclusions, title, sheet)


def main():
    keydir = "/home/robertdavidwest/"
    keen_client = get_keen_client(keydir + 
        'keen-buzzworthy-aol.json')
    gdrive_client = get_gdrive_client(keydir +
             'gdrive-keen-buzzworthy-aol.json')

    tz_str = "US/Pacific"
    timeframe = "previous_48_hours"

    live_campaigns = get_campaigns(keen_client, timeframe, tz_str)
    exclusions = get_alert_exclusions(gdrive_client)
    exclusions = exclusions.merge(live_campaigns, on='campaign', how='outer')
    exclusions = exclusions.fillna("")
    exclusions = exclusions.drop_duplicates()
    update_exclusions_in_sheets(gdrive_client, exclusions)
    return exclusions


if __name__ == '__main__':
    ee =main()
