from airports import *

def weather_scraping():
    '''
    Retrieves weather data for each airport for the past three days.

    Reads airport codes from a text file and scrapes weather information for the past three days
    from each airport's dedicated weather page. It compiles this data into a list of weather details.

    Returns:
    - A list containing weather data for each airport. Each entry includes weather details
      and the corresponding airport name.
    '''
    airports_list_txt = "all_airports_list.txt"

    # List containing all rows of weather
    output = []

    # Looping over airports list
    with open(airports_list_txt, 'r') as airports_list:
        for airport in airports_list:
            # Create a new instance of the web browser
            driver = webdriver.Edge()

            # Maximize the browser window to full screen
            driver.maximize_window()

            airport_name = list(airport.split('/'))[-1]

            link = airport + '/weather'
            try:
                # Navigate to the web page
                driver.get(link)

                # Click on the alert button
                alert_click(driver, 'onetrust-accept-btn-handler')

                try:
                    # Explicit wait for the table to be present
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "tbody"))
                    )

                    # Find all the 'tr' elements with class 'slave'
                    slave_rows = driver.find_elements(By.CSS_SELECTOR, 'tr.slave')

                    # Find all the 'tr' elements with class 'master'
                    master_rows = driver.find_elements(By.CSS_SELECTOR, 'tr.master')
                    
                    # For each 'slave' row, find the 'li' elements and extract their text
                    for i in range(len(slave_rows)):
                        # Since the 'li' elements are hidden, we may need to make them visible first
                        # This JavaScript snippet will change the display style of the parent 'ul'
                        driver.execute_script("arguments[0].style.display = 'block';", slave_rows[i])
        
                        # Now find all the 'li' elements within this 'row'
                        list_items = slave_rows[i].find_elements(By.TAG_NAME, 'li')
        
                        # Extract the text from each 'li' element
                        list_contents = [item.text for item in list_items if item.text.strip() != ''] + [' '.join(master_rows[i].text.split(' ')[-1:-3:-1])] + [airport_name]

                        if list_contents:
                            output.append(list_contents)                       
                    
                except NoSuchElementException as e:
                    print(f"Element not found: {e}")
                except TimeoutException as e:
                    print(f"Timeout waiting for element: {e}")
                except Exception as e:
                    print(f"An error occurred: {e}")
                finally: continue
            finally:
                    # Close the web browser
                    driver.quit()
    return output 

if __name__ == "__main__":
    '''
    Get the weather of each airport for the past three days and save it to weather.csv. 
    '''

    output = weather_scraping()
    
    # Open the CSV file in append mode
    with open('weather.csv', 'a') as file:
        for row in output:
            file.write(','.join(row) + '\n')
    