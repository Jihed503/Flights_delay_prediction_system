from datetime import datetime, timedelta
import time
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
    
    # Create a new instance of the web browser
    driver = webdriver.Edge()

    # Maximize the browser window to full screen
    driver.maximize_window()
    
    # Looping over airports list
    with open(airports_list_txt, 'r') as airports_list:
        for airport in airports_list:

            airport_name = list(airport.split('/'))[-1].strip()

            # Calculate the start and end dates for the last year
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365) #  datetime(2023, 11, 15) #  

            # Current date for the loop, starting from start_date
            current_date = start_date
            
            while current_date  <= end_date:
                # Format the URL with the correct year and month
                link = f"https://www.wunderground.com/history/daily/{airport_name}/date/{current_date.year}-{current_date.month}-{current_date.day}"
                
                try:
                    # Navigate to the web page
                    driver.get(link)

                    # Explicit wait for the table to be present
                    table = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//table[@aria-labelledby="History observation"]'))
                    )

                    # Extract all rows from the table
                    rows = table.find_elements(By.TAG_NAME, 'tr')
                    
                    # Loop through rows
                    for row in rows:
                        # Find all cells within the row
                        cells = row.find_elements(By.TAG_NAME, "td")  
                        line_list = [cell.text for cell in cells] + [airport_name] + [current_date]
                        
                        ########----------------------------Producer----------------------------########
                        # Convert the row to a JSON string
                        message = json.dumps(line_list, default=str)
                        # Send the message to a Kafka topic, with a callback for delivery reports
                        producer.produce('weather_data_topic', value=message, callback=delivery_report)

                        # Trigger any available delivery report callbacks from previous produce() calls
                        producer.poll(0)                 
                        
                except NoSuchElementException as e:
                    print(f"Element not found: {e}")
                except TimeoutException as e:
                    print(f"Timeout waiting for element: {e}")
                    break
                except KeyboardInterrupt:
                    exit()
                except Exception as e:
                    print(f"An error occurred: {e}")
                    
                # Increment the current_date by one day
                current_date += timedelta(days=1)
            
    # Close the web browser
    driver.quit()

    # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
    producer.flush()