import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

def scrape_oscars_data(delay=60):
    """
    Scrapes the Oscars Award database from the first until the
    latest awarding year ceremony using Selenium

    Returns the page source of the site with the final results
    """

    options = webdriver.FirefoxOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')

    driver = webdriver.Firefox(options=options)
    driver.get("https://awardsdatabase.oscars.org/") 

    #select award categories
    driver.find_element(By.XPATH,
                        "//button[contains(@class,'awards-basicsrch-awardcategory')]")\
                        .click()
    driver.find_element(By.XPATH,
                        "//b[contains(text(),'Current Categories')]")\
                        .click()

    #select starting award year
    driver.find_element(By.XPATH,
                        "//button[contains(@class,'awards-advsrch-yearsfrom')]")\
                        .click()
    driver.find_element(By.XPATH,
                        "//div[@class='btn-group multiselect-btn-group open']//input[@value='1']")\
                        .click()

    #select ending award year
    driver.find_element(By.XPATH,
                        "//button[contains(@class,'awards-advsrch-yearsto')]")\
                        .click()
    year_latest = len(driver.find_elements(By.XPATH,
                                           "//div[@class='btn-group multiselect-btn-group open']//li"))-2
    driver.find_element(By.XPATH,
                        f"//div[@class='btn-group multiselect-btn-group open']//input[@value='{year_latest}']")\
                        .click()

    #search to view results
    driver.find_element(By.XPATH,'//*[@id="btnbasicsearch"]').click()

    #wait for all results to show
    time.sleep(delay)

    try:
        #resultscontainer will contain all our needed Oscars data
        driver.find_element(By.XPATH,'//*[@id="resultscontainer"]')

        #get html source for BeautifulSoup extraction
        page_source = driver.page_source

    except NoSuchElementException as error:
        print(error)
        print(f"Needed id still not found after {delay} seconds delay")

    #close driver
    driver.close()
    print("Driver closed")

    return page_source


def clean_oscars_data(page_source):

    soup = BeautifulSoup(page_source, "lxml")
    results_container = soup.find('div', {'id':'resultscontainer'})

    return results_container

if __name__=="__main__":
    scrape_oscars_data()