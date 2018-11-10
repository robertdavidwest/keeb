from time import sleep
import warnings

from run_alerts import main

alert_log = None

run_frequency = 60*5 # in seconds
while True:
    warnings.warn("checking for alerts")
    alert_log = main(alert_log)
    warnings.warn("sleeping for %s seconds" % run_frequency)
    sleep(run_frequency)
