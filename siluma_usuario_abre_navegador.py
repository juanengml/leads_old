from selenium import webdriver 
from selenium.webdriver.support.ui import Select 
import time

driver = webdriver.Chrome() 
driver.get("http://qualicorp.com.br")

driver.find_element_by_link_text("Para você e sua familia").click()
driver.find_element_by_link_text("Diferenciais Quali").click() 
handles = driver.window_handles 

for i in handles: 
    driver.switch_to.window(i) 
    time.sleep(2) 
    driver.close()