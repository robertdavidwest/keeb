import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def _get_aol_data(browser, url, username, password):

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
    basic_button = [button for button in allNavButtons if button.text == 'Basic'][0]
    basic_button.click()

    # select video traffic by id
    elem = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'navText')))
    allNavText = browser.find_elements_by_class_name('navText')
    vid_button = [button for button in allNavText if button.text == 'Video traffic by Site and Video'][0]
    vid_button.click()

    # change number of rows displayed to all data
    elem = wait.until(
        EC.element_to_be_clickable((By.NAME, 'reportBlock_1_dataTable_length'))
    )

    elem.send_keys("All")
    elem.click()

    # wait untill all data is displayed
    rows_displayed = browser.find_element_by_id('reportBlock_1_dataTable_info').text
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


def get_aol_data(username, password):
    url = 'http://portal.aolonnetwork.com'
    #chromeDriverPath = "/home/robertdavidwest/keen/chromedriver"
    #browser = webdriver.Chrome(executable_path=chromeDriverPath)

    # Start up an Xvfb display
    from pyvirtualdisplay import Display
    display = Display(visible=0, size=(800, 600))
    display.start()

    # Load a Firefox selenium webdriver session
    browser = webdriver.Firefox()

    try:
        return _get_aol_data(browser, url, username, password)
    finally:
        browser.quit()