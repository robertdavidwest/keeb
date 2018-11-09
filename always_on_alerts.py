from time import sleep
import warnings

from run_alerts import main

run_frequency = 60 # in seconds
while True:
    warnings.warn("checking for alerts")
    main()
    warnings.warn("sleeping for %s seconds" % run_frequency)
    sleep(run_frequency)
