from confluent_kafka import Producer
import json
from flights import *

def delivery_report(err, msg):
    '''
    Prints the delivery status of a message.

    Parameters:
    - err: Error object, if any, or None.
    - msg: The message object.

    Outputs a failure message with the error or a success message with the message's topic and partition.
    '''
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Producer configuration
conf = {'bootstrap.servers': 'localhost:9092'}

# Create Producer instance
producer = Producer(conf)

##########********************************************************##########
##########--------------------Flights Scraping--------------------##########
##########********************************************************##########
airports_list_txt = "./web_scraping/all_airports_list.txt"

# Create a new instance of the web browser
driver = webdriver.Edge()

# Maximize the browser window to full screen
driver.maximize_window()

# Looping over airports list
with open(airports_list_txt, 'r') as airports_list:
    for airport in airports_list:
        airport_name = list(airport.split('/'))[-1].strip()

        for type in ['arrivals']:
            try:
                link = airport + '/' + type

                

                # Navigate to the web page
                driver.get(link)

                # Click on the alert button
                alert_click(driver, 'onetrust-accept-btn-handler')
                
                # Load earlier flights
                while True:
                    try:
                        button = WebDriverWait(driver, 30).until(
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
                        WebDriverWait(driver, 30).until(
                            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Load later flights')]"))
                        )
                        button = driver.find_element(By.XPATH, "//button[contains(text(), 'Load later flights')]")
                        
                        # Use JavaScript to click the button because an ad receives the click always
                        driver.execute_script("arguments[0].click();", button)

                    except Exception as e:
                        # If the button is not found, print a message and continue
                        print("Load later flights button not found, continuing without clicking.\n ",e)
                        break
            
                try:
                    # Explicit wait for the table to be present
                    WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.TAG_NAME, "tbody"))
                    )
                    # Locate the table
                    table = driver.find_element(By.TAG_NAME, "tbody")

                    if table:
                        # Find all rows in the table
                        rows = table.find_elements(By.TAG_NAME, "tr")

                        date = rows[0].find_elements(By.TAG_NAME, "td")[0].text.replace(',', '')
                        # Loop through rows
                        for row in rows[1:]:
                            # Find all cells within the row
                            cells = row.find_elements(By.TAG_NAME, "td")  
                            if len(cells)>1:  # Rows with cells
                                line_list = [cell.text for cell in cells]
                                if line_list[-1] != "Scheduled":
                                    ########----------------------------Producer----------------------------########
                                    # Convert the row to a JSON string
                                    message = json.dumps(airport_name + ',' + ','.join(line_list) + ',' + date + ',' + type + '\n')
                                    # Send the message to a Kafka topic, with a callback for delivery reports
                                    producer.produce('flights_data_topic', value=message, callback=delivery_report)

                                    # Trigger any available delivery report callbacks from previous produce() calls
                                    producer.poll(0)
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

            except:
                # Print the airport where an error has occured
                print(airport + '\n')
# Close the web browser
driver.quit()


# Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
producer.flush()