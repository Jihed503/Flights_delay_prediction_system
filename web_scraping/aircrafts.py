import re
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

def alert_click(driver, id):
    '''
    Attempts to click a button with a specified ID within 10 seconds.

    Parameters:
    - driver: Selenium WebDriver instance for browser interaction.
    - id: String specifying the ID of the button to click.

    If the button is not clickable within 10 seconds, it prints an error message and continues.
    '''
    try:
        button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, id))
                )
        button.click()
    except Exception as e:
        # If the button is not found, print a message and continue
        print("Button not found, continuing without clicking.\n" + '{}'.format(e))

def aircrafts_links():
    ##########******************************************************##########
    ##########--------------------Aircraft links--------------------##########
    ##########******************************************************##########
    aircrafts_families_link = "https://www.flightradar24.com/data/aircraft"

    # Setup WebDriver using Edge
    driver = webdriver.Edge()

    try:
        # Navigate to the main page where links are listed
        driver.get(aircrafts_families_link)

        alert_click(driver, 'onetrust-accept-btn-handler')

        # XPath to find <a> elements with a title attribute equal to 'Aircraft family code'
        xpath = "//a[@title='Aircraft family code']"
        
        # Find all <a> elements matching the XPath
        matching_links = driver.find_elements(By.XPATH, xpath)

        # Extract the href attribute from each link
        urls = [link.get_attribute('href') for link in matching_links]  

        for url in urls:
            print(url)  
        
        aircrafts_urls = []
        for url in urls:
            driver.get(url)
            aircrafts_urls_by_family = [link.get_attribute('href') for link in driver.find_elements(By.CLASS_NAME, "regLinks")]
            aircrafts_urls.extend(aircrafts_urls_by_family)
        
        # Save aircrafts list in a text file
        with open('./web_scraping/all_aircrafts_list.txt', 'w') as file:
            for url in aircrafts_urls:
                file.write(f"{url}\n")
            
        
    except Exception as e:
        print("Error during scraping aircraft urls")
        print(e)

    finally: 
        # Close the browser window
        driver.quit()

    return aircrafts_urls

def aircrafts_info():
    ##########************************************************************##########
    ##########--------------------Aircarft information--------------------##########
    ##########************************************************************##########
    aircrafts_families_link = "https://www.flightradar24.com/data/aircraft"

    # Setup WebDriver using Edge
    driver = webdriver.Edge()
    driver.maximize_window()
    
    try:
        # Navigate to the main page where links are listed
        driver.get(aircrafts_families_link)

        alert_click(driver, 'onetrust-accept-btn-handler')
        time.sleep(30) # login

        # XPath to find <a> elements with a title attribute equal to 'Aircraft family code'
        xpath = "//a[@title='Aircraft family code']"
        
        # Find all <a> elements matching the XPath
        matching_links = driver.find_elements(By.XPATH, xpath)

        # Extract the href attribute from each link
        urls = [link.get_attribute('href') for link in matching_links]  

        for url in urls[6:]:
            driver.get(url)

            # Explicit wait for the table to be present
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "tbody"))
            )

            # Locate the table
            table = driver.find_element(By.TAG_NAME, "tbody")

            if table:
                # Find all rows in the table
                rows = table.find_elements(By.TAG_NAME, "tr")

                # Loop through rows
                for row in rows:
                    # Find all cells within the row
                    cells = row.find_elements(By.TAG_NAME, "td")  
                    line_list = [cell.text for cell in cells]
                    # Save aircrafts info in a csv file
                    with open('./data/history/aircrafts_info.csv', 'a', encoding="utf-8") as file:
                        file.write(f"{','.join(line_list)}\n")
        
    except Exception as e: print(e)
    finally: 
        # Close the browser window
        driver.quit()

    
     

if __name__ == "__main__":
    '''
    Save all aircrafts urls in a text file named all_aircrafts_list.txt
    and save all aircrafts info in a csv file named aircrafts_info.csv.
    '''
    aircrafts_info()


    
            