import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time


def _get_aol_data(browser, url, username, password, timeframe):
    """User selenium to scrape data from the portal"""

    browser.get(url)
    wait = WebDriverWait(browser, 60)

    # login
    elem = wait.until(EC.element_to_be_clickable((By.ID,'email')))
    elem.send_keys(username)
    elem = browser.find_element_by_id('password')
    elem.send_keys(password + Keys.RETURN)

    # open analyize tab
    elem = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'tab')))
    alltabs = browser.find_elements_by_class_name('tab')
    analyize_tab = [tab for tab in alltabs if tab.text == 'ANALYZE'][0]
    analyize_tab.click()

    # display basic options
    elem = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'navSection')))
    allNavButtons = browser.find_elements_by_class_name('navSection')
    revenue_button = [button for button in allNavButtons if button.text == 'Revenue'][0]
    revenue_button.click()

    # select video traffic by id
    elem = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'navText')))
    allNavText = browser.find_elements_by_class_name('navText')
    vid_button = [button for button in allNavText if button.text == 'Revenue By Video - Site Publisher'][0]
    vid_button.click()

    # click away to hide current selection pane again
    elem = elem = wait.until(EC.element_to_be_clickable((By.ID, 'reportTemplate')))
    elem.click()

    # update time filter
    elem = wait.until(EC.element_to_be_clickable((By.ID, 'dateTimePicker')))
    elem.click()

    wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'ranges')))
    ranges_list = browser.find_elements_by_class_name('ranges')

    if len(ranges_list) > 1:
        raise AssertionError("More than 1 range class. Update scraper")
    ranges = ranges_list[0].find_elements_by_xpath(".//*")
    desired_range = [r for r in ranges if timeframe == r.text ][0]
    desired_range.click()

    # change number of rows displayed to all data
    elem = wait.until(
        EC.element_to_be_clickable((By.NAME, 'reportBlock_1_dataTable_length'))
    )

    elem.send_keys("All")
    elem.click()

    # wait untill all data is displayed
    rows_displayed = browser.find_element_by_id('reportBlock_1_dataTable_info').text

    # if the this text does not contain the word "of" then all data is already
    # displayed and we can scrape right away. Otherwise we wait for "All"
    # data to populate

    if 'of' in rows_displayed:
        words = rows_displayed.split() # should be of format [u'Showing', u'1-46', u'of', u'46']
        total_rows = words[-1]
        expected_phrase = "Showing 1-{} of {}".format(total_rows, total_rows)
        locator = (By.ID, 'reportBlock_1_dataTable_info')
        elem = wait.until(EC.text_to_be_present_in_element(locator, expected_phrase))

    # get data
    table = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'reportTableContainer')))
    html_table = table.get_attribute('innerHTML')
    data = pd.read_html(html_table)[0]
    return data


def get_aol_data(username, password, timeframe, driver_type):
    """
    username: str
        aol portal username
    password: str
        aol portal password
    timeframe: str:
        Here are the AOL portal options:
        "Today", "Yesterday", "This Week", "This Month", "Last 24 hours",
        "Last 48 hours", "Last 7 Days"
    driver_type: str
        "chrome" or "firefox". The chrome drive is saved directly in this
        repo and should be used for debugging. The firefox driver is
        configured to work on the pythonanwhere server

    Scrape AOL contentplay and prerollplay data from the portal for the
    specified timeframe

    """
    url = 'http://portal.aolonnetwork.com'

    if driver_type == 'chrome':
        chromeDriverPath = "/Users/rwest/Downloads/chromedriver"
        browser = webdriver.Chrome(executable_path=chromeDriverPath)
    elif driver_type == 'firefox':
        # Start up an Xvfb display
        from pyvirtualdisplay import Display
        display = Display(visible=0, size=(800, 600))
        display.start()

        # Load a Firefox selenium webdriver session
        browser = webdriver.Firefox()

    try:

        if timeframe == "This Month":
            timeframe = "June"
        data = _get_aol_data(browser, url, username, password, timeframe)
        data.rename(columns={'Video id': 'vidid',
                             'Billable video views': 'contentplay',
                             'Billable ad views': 'prerollplay'}, inplace=True)

        data = data[['vidid', 'Video title', 'contentplay', 'prerollplay']]

        if data.vidid.duplicated().any():
            raise AssertionError('aol data contains duplicate vidids')

        return data
    finally:
        browser.quit()