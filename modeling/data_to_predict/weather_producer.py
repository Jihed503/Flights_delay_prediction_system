import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
from confluent_kafka import Producer
 
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
 
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
 
url = "https://weather.com/weather/hourbyhour/l/a657ae7fb4a7b8c0a13cd8091d46be7753b095618480f28d9694b962a0363f22"
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')
 
# Récupérer toutes les dates disponibles
date_tags = soup.find_all('h2', class_='HourlyForecast--longDate--J_Pdh')
dates = [tag.text.strip() for tag in date_tags]
 
# Récupérer les heures et associer aux dates correspondantes
hours_tags = soup.find_all('h2', class_='DetailsSummary--daypartName--kbngc')
hours = [tag.text.strip() for tag in hours_tags if tag.text.strip()]
 
# Récupérer les informations détaillées pour chaque heure
forecast_details = []
detail_tags = soup.find_all('details', class_='DaypartDetails--DayPartDetail--2XOOV')
 
for detail in detail_tags:
    details = {}
    list_items = detail.find_all('li', class_='DetailsTable--listItem--Z-5Vi')
   
    for item in list_items:
        label_span = item.find('span', class_='DetailsTable--label--1qspW')
        value_span = item.find('span', class_='DetailsTable--value--2YD0-')
        label = label_span.text.strip() if label_span else None
        value = value_span.text.strip() if value_span else None
       
        if label and value:
            if "Wind" in label:
                wind_values = item.find('span', class_='Wind--windWrapper--3Ly7c')
                if wind_values:
                    wind_direction = wind_values.find_all('span')[0].text.strip()
                    wind_speed = wind_values.find_all('span')[1].text.strip()
                    wind_unit = wind_values.find('span', class_='DetailsTable--description--T7cw8').text.strip()
                    details['wind_direction'] = wind_direction
                    details['wind_speed'] = f"{wind_speed} {wind_unit}"
            elif "Humidity" in label:
                details['humidity'] = value
            elif "UV Index" in label:
                uv_values = item.find_all('span', class_='DetailsTable--value--2YD0-')
                if len(uv_values) == 2:
                    details['uv_index'] = uv_values[1].text.strip()
            else:
                details[label.lower().replace(' ', '_')] = value
   
    forecast_details.append(details)
 
# Associer les dates aux heures
date_hour_pairs = []
date_index = 0
current_date = datetime.strptime(dates[date_index], '%A, %B %d')
 
for hour in hours:
    if 'am' in hour and 'pm' in date_hour_pairs[-1][1] if date_hour_pairs else 'pm':
        date_index += 1
        current_date += timedelta(days=1)
    date_hour_pairs.append((current_date.strftime('%A, %B %d'), hour))
 
# Envoi des données à Kafka et écriture dans un fichier CSV

for i, details in enumerate(forecast_details):
    date, hour = date_hour_pairs[i]
    details_str = json.dumps(details)
    
    
    # Formater la date et l'heure dans un format compréhensible par Spark
    formatted_date = datetime.strptime(f"{date} {hour}", '%A, %B %d %I %p')
    formatted_date = formatted_date.replace(year=datetime.now().year)  # Fixer l'année à 2024
    formatted_date_str = formatted_date.strftime('%Y-%m-%d %H:%M:%S')
    
    # Envoi à Kafka
    message = json.dumps({'date': formatted_date_str, 'hour': hour, 'details': details})
    producer.produce('rt_weather_data_topic', value=message, callback=delivery_report)
    producer.poll(0)
 
producer.flush()
 
# Affichage des informations
for i, details in enumerate(forecast_details):
    date, hour = date_hour_pairs[i]
    formatted_date = datetime.strptime(f"{date} {hour}", '%A, %B %d %I %p')
    formatted_date = formatted_date.replace(year=datetime.now().year)  # Fixer l'année à 2024
    formatted_date_str = formatted_date.strftime('%Y-%m-%d %H:%M:%S')
    print(f"{formatted_date_str}, {hour}: {details}")