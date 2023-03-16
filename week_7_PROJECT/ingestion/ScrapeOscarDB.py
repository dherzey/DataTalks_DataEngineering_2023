import time
from selenium import webdriver
from selenium.webdriver.common.by import By

options = webdriver.FirefoxOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument('--incognito')
options.add_argument('--headless')

driver = webdriver.Firefox(options=options)
driver.get("https://awardsdatabase.oscars.org/") 

#select award categories
driver.find_element(By.XPATH,"//button[contains(@class,'awards-basicsrch-awardcategory')]").click()
driver.find_element(By.XPATH,"//b[contains(text(),'Current Categories')]").click()

#select starting award year
driver.find_element(By.XPATH,"//button[contains(@class,'awards-advsrch-yearsfrom')]").click()
driver.find_element(By.XPATH,"//div[@class='btn-group multiselect-btn-group open']//input[@value='1']").click()

#select ending award year
driver.find_element(By.XPATH,"//button[contains(@class,'awards-advsrch-yearsto')]").click()
year_latest = len(driver.find_elements(By.XPATH,"//div[@class='btn-group multiselect-btn-group open']//li"))-2
driver.find_element(By.XPATH,f"//div[@class='btn-group multiselect-btn-group open']//input[@value='{year_latest}']").click()

#search to view results
driver.find_element(By.XPATH,'//*[@id="btnbasicsearch"]').click()

#wait for all results to show
time.sleep(60)

#get html source for BeautifulSoup extraction
page_source = driver.page_source

#close driver
driver.close()