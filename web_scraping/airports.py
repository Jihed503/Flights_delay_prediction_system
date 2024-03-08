from math import e
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

def alert_click(driver, id):
    '''
    Click on the alert button.

    Parameters
        ----------
        driver : WebDriver
            Selenium WebDriver
        tag : str
            Id of the button
    '''
    try:
        button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, id))
                )
        button.click()
    except Exception as e:
        # If the button is not found, print a message and continue
        print("Button not found, continuing without clicking.\n" + '{}'.format(e))

if __name__ == "__main__":
    '''
    Save all airports urls in a text file named all_airports_list.txt
    and save all airports info in a csv file named airports_info.csv.
    '''

    ##########*****************************************************##########
    ##########--------------------Airport links--------------------##########
    ##########*****************************************************##########
    countries_list_link = "https://www.flightradar24.com/data/airports"

    # Setup WebDriver using Edge
    driver = webdriver.Edge()

    try:
        # Navigate to the main page where links are listed
        driver.get(countries_list_link)

        alert_click(driver, 'onetrust-accept-btn-handler')

        countries_table = driver.find_element(By.TAG_NAME, 'tbody')

        # Find all link elements within the table
        country_links = countries_table.find_elements(By.TAG_NAME, "a")
    
        # Store the URLs of the links to visit and delete duplicates
        country_urls = list(set([link.get_attribute('href') for link in country_links][1:]))
    finally: 
        # Close the browser window
        driver.quit()


    # List of all airports
    airports = []
    # Iterate through each URL
    for country in ['https://www.flightradar24.com/data/airports/tunisia']: # country_urls:
        driver = webdriver.Edge()
        try:
            # Navigate to the URL
            driver.get(country) # type: ignore
            alert_click(driver, 'onetrust-accept-btn-handler')
            
            try:
                # Wait for the captcha to disappear
                airports_table = WebDriverWait(driver, 120).until(
                    EC.element_to_be_clickable((By.TAG_NAME, 'tbody'))
                )

                # Find all link elements within the table
                airport_links = airports_table.find_elements(By.TAG_NAME, "a")

            except NoSuchElementException:
                    print("Table not found, continuing without clicking.")
            except TimeoutException:
                    print("Timed out waiting for page to load")
            else:
                # Store the URLs of the links to visit
                airport_urls = [link.get_attribute('href') for link in airport_links \
                                if str(link.get_attribute('href'))[-1] != '#'] # delete useless urls (exp: https://www.flightradar24.com/data/airports/moldova#)
                
                # Add new list to the global airports list
                airports.extend(airport_urls)

        finally:
            # Close the browser window
            driver.quit()
        
    # Save airports lists in a text file
    with open('all_airports_list.txt', 'w') as file:
        for item in airports:
            file.write(f"{item}\n")
    
    
    ##########***********************************************************##########
    ##########--------------------Airport information--------------------##########
    ##########***********************************************************##########
    # Setup WebDriver using Edge
    driver = webdriver.Edge()
    driver.maximize_window()

    info = [] # Contains airports info
    try:
        for airport_link in airports[:1]:
            # Navigate to the airport's main page
            driver.get(airport_link)
            
            alert_click(driver, 'onetrust-accept-btn-handler')

            airport_info = []
            try:
                info_elements = driver.find_elements(By.CLASS_NAME, 'chart-center')
            except NoSuchElementException:
                    print("Elements not found, continuing without scraping.")
            except TimeoutException:
                    print("Timed out waiting for elements to load")    

            else:
                airport_info = [element.text for element in info_elements]\
                      + [list(airport_link.split('/'))[-1]] # Airport id
                     
            info.append(airport_info)
    except Exception as e: print(e)
    finally: 
        # Close the browser window
        driver.quit()

    # Save airports info in a csv file
    with open('airports_info.csv', 'w') as file:
        for line in info:
            file.write(f"{','.join(line)}\n")
            