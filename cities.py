import requests, re, os
from bs4 import BeautifulSoup

WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_cities_in_India_by_population"
LAT_LONG_URL = ["https://www.latlong.net/category/cities-102-15.html",
                "https://www.latlong.net/category/cities-102-15-2.html",
                "https://www.latlong.net/category/cities-102-15-3.html",
                "https://www.latlong.net/category/cities-102-15-4.html",
                "https://www.latlong.net/category/cities-102-15-5.html",
                "https://www.latlong.net/category/cities-102-15-6.html",
                "https://www.latlong.net/category/cities-102-15-7.html",
                "https://www.latlong.net/category/cities-102-15-8.html"]

def scrape_indian_cities_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        city_tables = soup.find_all('table', {'class':'wikitable'})
        cities_data = []
        for i, city_table in enumerate(city_tables):
            for row in city_table.find_all('tr')[1:]:
                columns = row.find_all('td')
                if i == 0:  # For the first table
                    city_name = columns[0].text.strip()
                    population = columns[1].text.strip()
                else:  # For the second table
                    city_name = columns[1].text.strip()
                    population = columns[2].text.strip()
                cities_data.append({
                    'city_name': city_name,
                    'population': population
                })
        return cities_data
    else:
        print("Failed to fetch data from Wikipedia.")
        return None
    
def scrape_indian_cities_lat_long(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.find_all('tr')
        geo_data = []
        for row in rows:
            cols = row.find_all('td')
            if cols:
                full_location = cols[0].find('a').text
                city_name = full_location.split(',',1)[0]
                latitude = cols[1].text.strip()
                longitude = cols[2].text.strip()
                geo_data.append({
                    'city_name' : city_name,
                    'latitude': latitude,
                    'longitude': longitude
                })
        return geo_data
    else:
        print("Failed to fetch data from Wikipedia.")
        return None
    
#Combine data
def combine_data(cities_data, geo_data):
    combined_data = {}
    for city_data, geo in zip(cities_data, geo_data):
        city_name = re.sub(r'\[.*\]', '', city_data.get('city_name', '')).strip()  # Remove anything within square brackets
        population = city_data.get('population', '0')
        latitude = geo.get('latitude', '0')
        longitude = geo.get('longitude', '0')
        combined = {'population': population, 'latitude': latitude, 'longitude': longitude}
        combined_data[city_name] = combined
    return combined_data

def weather(sorted_combined_data):
    # Define OpenWeather API key
    api_key = os.getenv('OPENWEATHER_API_KEY')

    # Loop over the cities in sorted_combined_data
    for i in range(len(sorted_combined_data)):
        city, data = sorted_combined_data[i]
        # Make a GET request to the OpenWeather API
        response = requests.get(f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}")

        # Check if the request was successful
        if response.status_code == 200:
            weather_data = response.json()

            # Extract the necessary data
            temperature = weather_data['main']['temp']
            humidity = weather_data['main']['humidity']
            wind_speed = weather_data['wind']['speed']
            weather_conditions = weather_data['weather'][0]['description']

            # Add the data to the city's dictionary
            data['temperature'] = temperature
            data['humidity'] = humidity
            data['wind_speed'] = wind_speed
            data['weather_conditions'] = weather_conditions

        # Update the dictionary in sorted_combined_data
        sorted_combined_data[i] = (city, data)
    return sorted_combined_data

# Main function to execute ETL pipeline
if __name__ == "__main__":
    # Scrape city data from Wikipedia
    cities_data = scrape_indian_cities_data(WIKIPEDIA_URL)

    # Scrape latitude and longitude data from latlong.net
    geo_data = []
    for url in LAT_LONG_URL:
        geo_data.extend(scrape_indian_cities_lat_long(url))

    # Combine the data
    combined_data = combine_data(cities_data, geo_data)

    # sorted_combined_data = sorted(combined_data.items(), key=lambda x: x[0])

    # sorted_combined_data = weather(sorted_combined_data)

    print(len(combined_data))

    # Print the combined data
    for city, data in combined_data.items():
        print(f"City: {city}")
        print(f"Population: {data['population']}")
        print(f"Latitude: {data['latitude']}")
        print(f"Longitude: {data['longitude']}")
        print()

# Narasapuram