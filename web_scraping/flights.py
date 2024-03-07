import time
from airports import *

if __name__ == "__main__":
    '''
    Save flight details of each airport in text files named arrivals/departures_flights.csv.
    '''

    
    airports_list_txt = "all_airports_list.txt"

        

    # Looping over airports list
    with open(airports_list_txt, 'r') as airports_list:
        for airport in ['https://www.flightradar24.com/data/airports/doh']: #airports_list:
            airport_name = list(airport.split('/'))[-1]

            for type in ['arrivals']:#, 'departures']:
                try:
                    link = airport + '/' + type

                    # Create a new instance of the web browser
                    driver = webdriver.Edge()

                    # Maximize the browser window to full screen
                    driver.maximize_window()

                    # Navigate to the web page
                    driver.get(link)

                    # Click on the alert button
                    alert_click(driver, 'onetrust-accept-btn-handler')

                    
                    # Load earlier flights
                    while True:
                        try:
                            button = WebDriverWait(driver, 10).until(
                                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Load earlier flights')]"))
                            )
                            # Use JavaScript to click the button because an ad receives the click always
                            driver.execute_script("arguments[0].click();", button)
                            
                        except Exception as e:
                            # If the button is not found, print a message and continue
                            print("Load earlier flights button not found, continuing without clicking.\n ",e)
                            break
                    
                    # Load later flights
                    while True:
                        try:
                            WebDriverWait(driver, 10).until(
                                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Load later flights')]"))
                            )
                            button = driver.find_element(By.XPATH, "//button[contains(text(), 'Load later flights')]")
                            
                            # Use JavaScript to click the button because an ad receives the click always
                            driver.execute_script("arguments[0].click();", button)

                        except Exception as e:
                            # If the button is not found, print a message and continue
                            print("Load later flights button not found, continuing without clicking.\n ",e)
                            break                
                    
                    '''
                    # Display the whole table
                    while True:
                        try:
                            # Use WebDriverWait to wait for the buttons to be present and clickable
                            buttons = WebDriverWait(driver, 10).until(
                                EC.visibility_of_all_elements_located((By.CLASS_NAME, 'btn-flights-load'))
                            )
        
                            if buttons:
                                for button in buttons:
                                    try:
                                        # Ensure the button is clickable before clicking
                                        WebDriverWait(driver, 5).until(EC.element_to_be_clickable(button))
                                        button.click()
                                        # Wait a bit for the data to load, adjust time as needed
                                        WebDriverWait(driver, 10).until(
                                            EC.staleness_of(button)
                                        )
                                    except TimeoutException:
                                        print("Button was not clickable.")
                                    except Exception as e: print(e)
                            else:
                                # If no buttons found, break from the loop
                                print("No more buttons to click.")
                                break
                        except TimeoutException:
                            # If buttons are not found within the timeout period, assume no more buttons to click
                            print("No buttons found or they took too long to appear.")
                            break
                        except Exception as e: print(e)
                        '''
                
                    try:
                        # Explicit wait for the table to be present
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.TAG_NAME, "tbody"))
                        )
                        # Locate the table
                        table = driver.find_element(By.TAG_NAME, "tbody")

                        if table:
                            # Find all rows in the table
                            rows = table.find_elements(By.TAG_NAME, "tr")

                            # Open the CSV file in append mode
                            with open('flights.csv', 'a') as file:
                                date = rows[0].find_elements(By.TAG_NAME, "td")[0].text.replace(',', '')
                                # Loop through rows
                                for row in rows[1:]:
                                    # Find all cells within the row
                                    cells = row.find_elements(By.TAG_NAME, "td")  
                                    if len(cells)>1:  # Rows with cells
                                        line_list = [cell.text for cell in cells if cell.text]
                                        if line_list[-1] != "Scheduled":
                                            # Write the cell text to the CSV file
                                            file.write(airport_name + ',' + ','.join(line_list) + ',' + date + ',' + type + '\n')
                                    else: 
                                        #file.write(cells[0].text.replace(',', '') + '\n') # Write the date
                                        date = cells[0].text.replace(',', '')
                

                    
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



    

















