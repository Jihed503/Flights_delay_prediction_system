from airports import *

if __name__ == "__main__":
    '''
    Save flight details of each airport in text files named arrivals/departures_flights.csv.
    '''

    
    airports_list_txt = "all_airports_list.txt"

        

    # Looping over airports list
    with open(airports_list_txt, 'r') as airports_list:
        for airport in airports_list:
            airport_name = list(airport.split('/'))[-1]

            for type in ['arrivals', 'departures']:
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
                
                    # Display the whole table
                    for i in range(10):
                        try:
                            # wail until the button is loaded
                            button = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.CLASS_NAME, 'btn-flights-load'))
                            )
                            button.click()
                        except:
                            # If the button is not found, print a message and continue
                            print("Button not found, continuing without clicking.")
                            break
                
                    try:
                        # Explicit wait for the table to be present
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.TAG_NAME, "tbody"))
                        )
                        # Locate the table
                        table = driver.find_element(By.TAG_NAME, "tbody")

                        # Find all rows in the table
                        rows = table.find_elements(By.TAG_NAME, "tr")

                        # Open the CSV file in append mode
                        with open('arrivals_flights.csv', 'a') as file:
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



    

















