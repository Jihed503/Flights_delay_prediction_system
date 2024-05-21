from airports import *

def reviews_scraping():
    '''
    Collects user reviews for each airport from a list and returns them as a list.

    This function reads an airport list from a file, navigates to the review page for each,
    and scrapes user reviews. It tries to load all available reviews by clicking the load more button
    up to 20 times for each airport.

    Returns:
    - A list of strings, each containing a user review followed by the airport name.
    '''

    airports_list_txt = "all_airports_list.txt"

    # List containing all users reviews
    reviews = []

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
                    
                    # For each comment add the airport name and append to reviews list
                    for comment in comments:
                        reviews.append(comment.text.replace(',', ' ') + ',' + airport_name + '\n')

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

    return reviews 


if __name__ == "__main__":
    '''
    Get the user reviews of each airport of all time and save it to reviews.csv. 
    '''

    reviews = reviews_scraping()

    # Open the CSV file in append mode
    with open('reviews.csv', 'a') as file:
        for row in reviews:
            file.write(row)
    
    
    