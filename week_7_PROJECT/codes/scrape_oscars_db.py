"""----------------------------------------------------------------------
Script for web scraping data in the Oscar Academy Awards database
Last modified: March 2023
----------------------------------------------------------------------"""

import re
import time
import numpy as np
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
    xpath = "//button[contains(@class,'awards-basicsrch-awardcategory')]"
    driver.find_element(By.XPATH, xpath).click()

    xpath = "//b[contains(text(),'Current Categories')]"
    driver.find_element(By.XPATH, xpath).click()

    #select starting award year
    xpath = "//button[contains(@class,'awards-advsrch-yearsfrom')]"
    driver.find_element(By.XPATH, xpath).click()

    xpath = "//div[@class='btn-group multiselect-btn-group open']//input[@value='1']"
    driver.find_element(By.XPATH, xpath).click()

    #select ending award year
    xpath = "//button[contains(@class,'awards-advsrch-yearsto')]"
    driver.find_element(By.XPATH, xpath).click()

    xpath = "//div[@class='btn-group multiselect-btn-group open']//li"
    latest = len(driver.find_elements(By.XPATH, xpath))-2

    xpath = f"//div[@class='btn-group multiselect-btn-group open']//input[@value='{latest}']"
    driver.find_element(By.XPATH, xpath).click()

    #search to view results
    driver.find_element(By.XPATH, '//*[@id="btnbasicsearch"]').click()

    #wait for all results to show
    time.sleep(delay)

    try:
        #resultscontainer will contain all our needed Oscars data
        driver.find_element(By.XPATH, '//*[@id="resultscontainer"]')

        #get html source for BeautifulSoup extraction
        page_source = driver.page_source

    except NoSuchElementException as error:
        print(error)
        print(f"Needed element still not found after {delay} seconds delay")

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

    class_ = 'awards-result-chron result-group group-awardcategory-chron'
    award_year_all = results_container.find_all('div', class_=class_)

    oscars_results_dict = {}
    for award_year_group in award_year_all:

        #find the award year title
        award_year = award_year_group.find('div', class_='result-group-title')\
                                     .get_text(strip=True)       
 
        #award category result subgroup (each contains award title and nominees)
        class_ = 'result-subgroup subgroup-awardcategory-chron'
        award_category_all = award_year_group.find_all('div', class_=class_)

        award_subgroup_dict = {}
        for award_category_group in award_category_all:

            #dictionary to contain movie lists
            movies_dict = {"nominated":[], "won":[]}

            #find award title
            award_title = award_category_group.find('div', class_='result-subgroup-title')\
                                              .get_text(strip=True)            
            try:
                #find nominated movies
                movies = [movie.get_text(strip=True) for movie in award_category_group\
                               .find_all('div', class_='awards-result-film-title')]
                
                #find winning movie
                winner_group = award_category_group.find('span', {'title':'Winner'})\
                                                   .find_next_sibling('div')
                
                movies_dict["won"] = [movie.get_text(strip=True) for movie in winner_group\
                                           .find_all('div', class_='awards-result-film-title')] 
                
                #remove duplicates and the winner from nominated list
                movies_dict["nominated"] = list(set(movies) - set(movies_dict["won"]))     

            except AttributeError:
                pass

            award_subgroup_dict[award_title] = movies_dict

        oscars_results_dict[award_year] = award_subgroup_dict

    return oscars_results_dict


def get_raw_dataframe(oscars_results_dict):
    """
    Using the raw dictionary data from extract_oscars_data(), this
    function will read the dictionary object and turn it into the 
    structured format of a dataframe

    Returns a dataframe with the following columns:
        - AwardYear: the year the award was received
        - AwardCeremonyNum: the nth annual ceremony award
        - Movie: the title of the nominated film
        - AwardCategory: the category the film was nominated for
        - AwardStatus: whether the film was only nominated or had won
    """

    df_list = []
    for key, values in oscars_results_dict.items():

        df_structure = {
            "AwardYear":'',
            "AwardCeremonyNum":'',
            "Movie":[],
            "AwardCategory":[],
            "AwardStatus":[]
        }

        #separate key strings to extract year
        key_split = key.split(" ")
        df_structure['AwardYear'] = key_split[0]
        df_structure['AwardCeremonyNum'] = re.findall(r'\d+',key_split[1])[0]

        #extract all movies per award year
        for categ, movies in values.items():
            movies_concat = sum(list(movies.values()),[])
            count = len(movies_concat)

            if count > 0:
                df_structure['Movie'].extend(movies_concat)
                df_structure['AwardCategory'].extend(list(np.repeat([categ],count)))

                for status, movie in movies.items():
                    df_structure['AwardStatus'].extend(list(np.repeat([status],len(movie))))
        
        #append dataframe to list
        df_list.append(pd.DataFrame(df_structure))

    #concatenate and return all award years dataframe into one    
    return pd.concat(df_list)


if __name__=="__main__":
    scrape_oscars_data()