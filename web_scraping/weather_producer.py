from confluent_kafka import Producer
import json
from weather import *

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

if __name__ == "__main__":
    # Producer configuration
    conf = {'bootstrap.servers': 'localhost:9092'}

    # Create Producer instance
    producer = Producer(conf)
    
    ##########********************************************************##########
    ##########--------------------Weather Scraping--------------------##########
    ##########********************************************************##########
    airports_list_txt = "./web_scraping/all_airports_list.txt"
    
    # Looping over airports list
    with open(airports_list_txt, 'r') as airports_list:
        for airport in airports_list:
            # Create a new instance of the web browser
            driver = webdriver.Edge()

            # Maximize the browser window to full screen
            driver.maximize_window()

            airport_name = list(airport.split('/'))[-1].strip()

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
                        list_contents = [item.text for item in list_items] + [' '.join(master_rows[i].text.split(' ')[-1:-3:-1])] + [airport_name]

                        ########----------------------------Producer----------------------------########
                        # Convert the row to a JSON string
                        message = json.dumps(list_contents)
                        # Send the message to a Kafka topic, with a callback for delivery reports
                        producer.produce('weather_data_topic', value=message, callback=delivery_report)

                        # Trigger any available delivery report callbacks from previous produce() calls
                        producer.poll(0)                     
                    
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
            finally:
                # Close the web browser
                driver.quit()

    # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
    producer.flush()