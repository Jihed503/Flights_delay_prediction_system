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

def airports_links():
    ##########*****************************************************##########
    ##########--------------------Airport links--------------------##########
    ##########*****************************************************##########
    countries_list_link = "https://www.flightradar24.com/data/airports"

    # Setup WebDriver using Edge
    driver = webdriver.Edge()
    country_urls = []
    try:
        # Combined list of African, European, and Middle Eastern countries
        countries = ['algeria', 'angola', 'benin', 'botswana', 'burkina faso', 'burundi',
                        'cabo-verde', 'cameroon', 'central-african-republic', 'chad', 'comoros',
                        'democratic-republic-of-the-congo',  'djibouti',
                        'egypt', 'equatorial-guinea', 'eritrea', 'eswatini', 'ethiopia', 'gabon',
                        'gambia', 'ghana', 'guinea', 'guinea-bissau', "ivory-coast",
                        'kenya', 'lesotho', 'liberia', 'libya', 'madagascar', 'malawi', 'mali',
                        'mauritania', 'mauritius', 'morocco', 'mozambique', 'namibia', 'niger',
                        'nigeria', 'rwanda', 'sao-tome-and-principe', 'senegal', 'seychelles',
                        'sierra-leone', 'somalia', 'south-africa', 'south-sudan', 'sudan',
                        'tanzania', 'togo', 'tunisia', 'uganda', 'zambia', 'zimbabwe', 'albania',
                        'andorra', 'armenia', 'austria', 'azerbaijan', 'belarus', 'belgium',
                        'bosnia-and-herzegovina', 'bulgaria', 'croatia', 'cyprus', 'czech-republic',
                        'denmark', 'estonia', 'finland', 'france', 'georgia', 'germany', 'greece',
                        'hungary', 'iceland', 'ireland', 'italy', 'kazakhstan', 'kosovo', 'latvia',
                        'liechtenstein', 'lithuania', 'luxembourg', 'malta', 'moldova', 'monaco',
                        'montenegro', 'netherlands', 'north macedonia', 'norway', 'poland',
                        'portugal', 'romania', 'russia', 'san-marino', 'serbia', 'slovakia',
                        'slovenia', 'spain', 'sweden', 'switzerland', 'turkey', 'ukraine',
                        'united-kingdom', 'bahrain', 'cyprus', 'iran', 'iraq',
                         'jordan', 'kuwait', 'lebanon', 'oman', 'palestine', 'qatar',
                        'saudi-arabia', 'syria', 'united-arab-emirates', 'yemen'
                    ]



        # Navigate to the main page where links are listed
        driver.get(countries_list_link)

        alert_click(driver, 'onetrust-accept-btn-handler')

        countries_table = driver.find_element(By.TAG_NAME, 'tbody')

        # Find all link elements within the table
        country_links = countries_table.find_elements(By.TAG_NAME, "a")
    
        
        # Store the URLs of the links to visit and delete duplicates
        #country_urls = list(set([link.get_attribute('href') for link in country_links if link.get_attribute('href').split('/') in countries][1:]))
        
        for link in country_links:
            #if link.get_attribute('href').split('/') in countries :
            print(str(link.get_attribute('href')).split('/')[-1])
            if str(link.get_attribute('href')).split('/')[-1] in countries:
                country_urls.append(link.get_attribute('href')) 
        
        country_urls = country_urls[::2] # Deleting duplicates
        print(country_urls)  
        print(len(country_urls))

    except:
         print("Error during scraping countries urls")

    finally: 
        # Close the browser window
        driver.quit()

    # Save airports lists in a text file
    with open('all_airports_list.txt', 'w') as file:
        # List of all airports
        #airports = []
        # Iterate through each URL
        for country in country_urls:
            driver = webdriver.Edge()
            try:
                # Navigate to the URL
                driver.get(country) # type: ignore
                alert_click(driver, 'onetrust-accept-btn-handler')
                
                try:
                    # Wait for the captcha to disappear
                    WebDriverWait(driver, 120).until(
                        EC.element_to_be_clickable((By.TAG_NAME, 'tbody'))
                    )

                    # Locate table rows that contain a <span> element
                    rows_with_span = driver.find_elements(By.XPATH, "//tr[.//span]")


                    # Iterate through each row to extract the URL
                    for row in rows_with_span:
                        # Assuming the URL is in an <a> tag directly within the <tr> that contains a <span>
                        # Adjust the XPath if the structure is different
                        try:
                            a_tag = row.find_element(By.XPATH, ".//a")
                            url = a_tag.get_attribute('href')
                            #airports.append(url)
                            file.write(f"{url}\n")
                        except:
                            # Handle the case where an <a> tag might not be present
                            print("No <a> tag found in this row, or other error.")
                    
                    # Find all link elements within the table
                    #airport_links = airports_table.find_elements(By.TAG_NAME, "a")

                except NoSuchElementException:
                        print("Table not found, continuing without clicking.")
                except TimeoutException:
                        print("Timed out waiting for page to load")
                #else:
                    # Store the URLs of the links to visit
                #    airport_urls = [link.get_attribute('href') for link in airport_links \
                #                    if str(link.get_attribute('href'))[-1] != '#'] # delete useless urls (exp: https://www.flightradar24.com/data/airports/moldova#)
                    
                    # Add new list to the global airports list
                #    airports.extend(airport_urls)

            finally:
                # Close the browser window
                driver.quit()
        
    

def airports_info():
    ##########***********************************************************##########
    ##########--------------------Airport information--------------------##########
    ##########***********************************************************##########
    # Setup WebDriver using Edge
    driver = webdriver.Edge()
    driver.maximize_window()
    info = [] # Contains airports info
    with open('all_airports_list.txt', 'r') as file:
        try:
            for airport_link in file:
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
     

if __name__ == "__main__":
    '''
    Save all airports urls in a text file named all_airports_list.txt
    and save all airports info in a csv file named airports_info.csv.
    '''
    airports_links()


    
            