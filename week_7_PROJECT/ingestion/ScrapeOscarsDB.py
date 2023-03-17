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
    driver.find_element(By.XPATH,
                        '//*[@id="btnbasicsearch"]')\
                        .click()

    #wait for all results to show
    time.sleep(delay)

    try:
        #resultscontainer will contain all our needed Oscars data
        driver.find_element(By.XPATH,
                            '//*[@id="resultscontainer"]')

        #get html source for BeautifulSoup extraction
        page_source = driver.page_source

    except NoSuchElementException as error:
        print(error)
        print(f"Needed id still not found after {delay} seconds delay")

    #close driver
    driver.close()
    print("Driver closed")

    return page_source


def extract_raw_data(page_source):
    """
    Function which parses the page_source using BeautifulSoup. All raw 
    results are extracted and saved as a dictionary object.
    
    Returns a dictionary object with award years as its main key. Each key 
    consists of the award category and the nominated/winning movie for each.
    See below sample structure of the dictionary output:

    {
        AwardYear_1: {
            AwardCategory_1: {
                nominated_films: [],
                winning_films: []
            }
        },
        AwardYear_2: ...
    }
    """

    soup = BeautifulSoup(page_source, "lxml")
    results_container = soup.find('div', {'id':'resultscontainer'})
    award_year_all = results_container.find_all('div',
                                                class_='awards-result-chron result-group group-awardcategory-chron')

    oscars_results_dict = {}
    for award_year_group in award_year_all:

        #find the award year title
        award_year = award_year_group.find('div',
                                           class_='result-group-title')\
                                     .get_text(strip=True)        
        #award category result subgroup (each contains award title and nominees)
        award_category_all = award_year_group.find_all('div',
                                                       class_='result-subgroup subgroup-awardcategory-chron')

        award_subgroup_dict = {}
        for award_category_group in award_category_all:

            #dictionary to contain movie lists
            movies_dict = {"nominated":[], 
                           "won":[]}
            #find award title
            award_title = award_category_group.find('div',
                                                    class_='result-subgroup-title')\
                                              .get_text(strip=True)            
            try:
                #find nominated movies
                movies = [movie.get_text(strip=True) for movie in award_category_group\
                               .find_all('div', 
                                         class_='awards-result-film-title')]
                #find winning movie
                winner_group = award_category_group.find('span', 
                                                         {'title':'Winner'})\
                                                   .find_next_sibling('div')
                movies_dict["won"] = [movie.get_text(strip=True) for movie in winner_group\
                                           .find_all('div', 
                                                     class_='awards-result-film-title')] 
                #remove duplicates and the winner from nominated list
                movies_dict["nominated"] = list(set(movies) - set(movies_dict["won"]))                
            except AttributeError:
                pass

            award_subgroup_dict[award_title] = movies_dict
        oscars_results_dict[award_year] = award_subgroup_dict

    return oscars_results_dict


def clean_raw_data(oscars_results_dict):

    return oscars_results_df


if __name__=="__main__":
    scrape_oscars_data()