import time
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
aircrafts_list_txt = "./web_scraping/all_aircrafts_list.txt"

# Create a new instance of the web browser
driver = webdriver.Edge()

# Maximize the browser window to full screen
driver.maximize_window()

# Looping over aircrafts list
with open(aircrafts_list_txt, 'r') as aircrafts_list:
    for i, aircraft_link in enumerate(aircrafts_list):
        aircraft_registration = list(aircraft_link.split('/'))[-1].strip()

        try:
            
            # Navigate to the web page
            driver.get(aircraft_link)
            
            # Click on the alert button
            alert_click(driver, 'onetrust-accept-btn-handler')

            ###########################Login###########################
            if i==0:
                username = 'iheb.benjeddi9573@gmail.com'
                password = 'IHEBjihedAziz2024!!?'

                try:
                    button = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.ID, "auth-button"))
                    )
                    # Use JavaScript to click the button because an ad receives the click always
                    driver.execute_script("arguments[0].click();", button)
                except Exception as e:
                    # If the button is not found, print a message and continue
                    print("Login button not found.\n ",e)
                
                # Find the username/email field and send the username
                username_field = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.NAME, 'email')))
                username_field.send_keys(username)

                # Find the password field and send the password
                password_field = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, 'props.name')))
                password_field.send_keys(password)

                time.sleep(2)

                # Find the login button and click it
                #login_button = driver.find_element_by_xpath("//button[contains(text(), 'Log in with email')]") 
                login_button = driver.find_element(By.CSS_SELECTOR, "button.w-full")
                driver.execute_script("arguments[0].click();", login_button)
            
            # Load earlier flights
            for _ in range(40):
                try:
                    button = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".loadButton.loadEarlierFlights.bottomMargin"))
                    )
                    # Use JavaScript to click the button because an ad receives the click always
                    driver.execute_script("arguments[0].click();", button)
                    time.sleep(4)
                    
                except Exception as e:
                    # If the button is not found, print a message and continue
                    print("Load earlier flights button not found, continuing without clicking.\n ",e)
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
                    for row in rows:
                        # Find all cells within the row
                        cells = row.find_elements(By.TAG_NAME, "td")  
                        line_list = [cell.text for cell in cells]
                        if line_list[-3] != "Scheduled":
                            ########----------------------------Producer----------------------------########
                            # Convert the row to a JSON string
                            message = json.dumps(aircraft_registration + ',' + ','.join(line_list) + '\n')
                            # Send the message to a Kafka topic, with a callback for delivery reports
                            producer.produce('flights_data_topic', value=message.encode('utf-8'), callback=delivery_report)

                            # Trigger any available delivery report callbacks from previous produce() calls
                            producer.poll(0)

            except NoSuchElementException as e:
                print(f"Element not found: {e}")
            except TimeoutException as e:
                print(f"Timeout waiting for element: {e}")
            except Exception as e:
                print(f"An error occurred: {e}")
            finally: continue
        except KeyboardInterrupt:
            exit()
        except:
            # Print the aircraft where an error has occured
            print(aircraft_registration + '***************************************************' + '\n')
# Close the web browser
driver.quit()


# Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
producer.flush()