from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import json
import time
from confluent_kafka import Producer
 
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
 
def flights_scraping():
    '''
    Gathers and returns flight details from a list of airports.
 
    Reads airport codes from a text file, scrapes flight information for each airport,
    and compiles a list of flights details including arrivals and departures.
 
    Returns:
    - A list of flight details, including airport name, date, and flight status.
    '''
    # Producer configuration
    conf = {'bootstrap.servers': 'localhost:9092'}
 
    # Create Producer instance
    producer = Producer(conf)
 
    airports_list_txt = "all_airports_list.txt"
 
    for _ in range(1): #while(1):
        # Looping over airports list
        with open(airports_list_txt, 'r') as airports_list:
            for airport in airports_list:
                airport_name = list(airport.split('/'))[-1]

                link = airport + '/arrivals'

                # Create a new instance of the web browser
                driver = webdriver.Edge()

                # Maximize the browser window to full screen
                driver.maximize_window()

                # Navigate to the web page
                driver.get(link)

                # Click on the alert button
                alert_click(driver, 'onetrust-accept-btn-handler')
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

                flights = []
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
                        # Loop through rows
                        for row in rows[1:]:
                            # Find all cells within the row
                            cells = row.find_elements(By.TAG_NAME, "td") 

                            if len(cells)>1:  # Rows with cells
                                line_list = [cell.text for cell in cells if cell.text]
                                if line_list[-1] == "Scheduled":
                                    flight = line_list[1]
                                    flights.append(flight)
                except NoSuchElementException as e:
                    print(f"Element not found: {e}")
                    continue
                except TimeoutException as e:
                    print(f"Timeout waiting for element: {e}")
                    continue
                except Exception as e:
                    print(f"An error occurred: {e}")
                    continue

                print(flights)
                
                for flight in flights:
                    if flight:
                        # Navigate to the web page
                        driver.get('https://www.flightradar24.com/data/flights/' + flight)
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

                                # Loop through rows
                                for row in rows:
                                    # Find all cells within the row
                                    cells = row.find_elements(By.TAG_NAME, "td")  
                                    line_list = [cell.text for cell in cells]
                                    if line_list[-3].split()[0] in ["Scheduled", "Estimated"]:
                                        ########----------------------------Producer----------------------------########
                                        # Convert the row to a JSON string
                                        message = json.dumps(flight + ',' + ','.join(line_list) + '\n')
                                        # Send the message to a Kafka topic, with a callback for delivery reports
                                        producer.produce('scheduled_flights_data_topic', value=message.encode('utf-8'), callback=delivery_report)

                                        # Trigger any available delivery report callbacks from previous produce() calls
                                        producer.poll(0)
                                        print("**********************************************************************************")

                        except NoSuchElementException as e:
                            print(f"Element nound: {e}")
                        except TimeoutException as e:
                            print(f"Timeout waiting for element: {e}")
                        except Exception as e:
                            print(f"An error occurred 1: {e}")
                
                # Close the web browser
                driver.quit()

        # Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
        producer.flush()
 
 
if __name__ == "__main__":
    flights_scraping()