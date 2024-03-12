from confluent_kafka import Producer
import json
from reviews import *

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
##########--------------------Reviews Scraping--------------------##########
##########********************************************************##########
airports_list_txt = "all_airports_list.txt"

# Looping over airports list
with open(airports_list_txt, 'r') as airports_list:
    for airport in airports_list:
        # Create a new instance of the web browser
        driver = webdriver.Edge()

        # Maximize the browser window to full screen
        driver.maximize_window()

        airport_name = list(airport.split('/'))[-1]

        link = airport + '/reviews'
        try:
            # Navigate to the web page
            driver.get(link)

            # Click on the alert button
            alert_click(driver, 'onetrust-accept-btn-handler')

            # Display the whole table
            for i in range(20):
                try:
                    # wail until the button is loaded
                    button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, 'btn-flights-load'))
                    )
                    # Use JavaScript to click the button because an ad receives the click always
                    driver.execute_script("arguments[0].click();", button)
                except:
                    # If the button is not found, print a message and continue
                    print("Button not found, continuing without clicking.")
                    break

            try:
                # Explicit wait for the comments to be present
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "aside.airport-review-data"))
                )

                # Find all the comments
                comments = driver.find_elements(By.CSS_SELECTOR, 'div.content')

                
                ########----------------------------Producer----------------------------########
                # For each comment add the airport name and append to reviews list
                for comment in comments:
                    # Convert the row to a JSON string
                    message = json.dumps(comment.text.replace(',', ' ') + ',' + airport_name + '\n')
                    # Send the message to a Kafka topic, with a callback for delivery reports
                    producer.produce('reviews_data_topic', value=message, callback=delivery_report)

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