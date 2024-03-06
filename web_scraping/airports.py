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
    except NoSuchElementException:
        # If the button is not found, print a message and continue
        print("Button not found, continuing without clicking.")

if __name__ == "__main__":
    '''
    Save all airports urls in a text file named all_airports_list.txt.
    '''

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
        for country in country_urls[:2]:
            driver = webdriver.Edge()
            try:
                # Navigate to the URL
                driver.get(country) # type: ignore
                alert_click(driver, 'onetrust-accept-btn-handler')
            
                try:
                    # Wait for the captcha to disappear
                    # airports_table = driver.find_element(By.TAG_NAME, 'tbody')
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
                    airport_urls = [link.get_attribute('href') for link in airport_links]
                
                    # Add new list to the global airports list
                    airports.extend(airport_urls)
            finally:
                # Close the browser window
                driver.quit()
        
        # Save airports lists in a text file
        with open('all_airports_list.txt', 'w') as file:
            for item in airports:
                # delete useless urls (exp: https://www.flightradar24.com/data/airports/moldova#)
                if item[-1] != '#':
                    file.write(f"{item}\n")
        