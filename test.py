import requests, os, json
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
# Create a new dictionary to store the combined data
    combined_data = {}

    # Add the population data to the combined_data dictionary
    for city in cities_data:
        city_name = city['city_name']
        population = city['population']
        combined_data[city_name] = {'population': population}

    # Add the latitude and longitude data to the combined_data dictionary
    for city in geo_data:
        city_name = city['city_name']
        latitude = city['latitude']
        longitude = city['longitude']
        if city_name in combined_data:
            # If the city is already in the combined_data dictionary, update the latitude and longitude
            combined_data[city_name]['latitude'] = latitude
            combined_data[city_name]['longitude'] = longitude

    combined_data = {city: data for city, data in combined_data.items() if 'population' in data and 'latitude' in data and 'longitude' in data}
    return combined_data

#Get Weather Data
def get_weather(lat, lon):
    # Define OpenWeather API key
    api_key = '6a0294130dd9aa0b952ab50dc3011868'
    # api_key = os.getenv('OPENWEATHER_API_KEY')

    try:
        # Make a GET request to the OpenWeather API
        response = requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}")

        # Check if the request was successful
        if response.status_code == 200:
            try:
                weather_data = response.json()

                # Extract the necessary data
                temperature = weather_data['main']['temp']
                humidity = weather_data['main']['humidity']
                wind_speed = weather_data['wind']['speed']
                weather_conditions = weather_data['weather'][0]['description']
                city_id = weather_data['id']

                # Return the data
                return {
                    'city_id': city_id,
                    'temperature': temperature,
                    'humidity': humidity,
                    'wind_speed': wind_speed,
                    'weather_conditions': weather_conditions
                }
            except json.JSONDecodeError:
                print("Error: Response is not valid JSON")
                return None
            except KeyError:
                print("Error: Some necessary data is missing in the response")
                return None
        else:
            print(f"Failed to get weather data for Latitude: {lat}, Longitude: {lon}. Status code: {response.status_code}, Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None

# Fetch station data   
def fetch_station_data(combined_data):
    url = "https://www.cleartrip.com/trains/stations/list"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the table with the station data
    table = soup.find('table')

    # Get the tbody element in the table
    tbody = table.find('tbody')

    # Get all rows in the table
    rows = tbody.find_all('tr')

    city_name = combined_data['city_name']

    for row in rows:
        # Get all columns in the row
        cols = row.find_all('td')

        # Get the station name from the first column
        city = cols[2].text.strip()

        # If the station name is a city in combined_data, add the station details to that city's data
        if city == city_name:
            # Get the station details from the other columns
            station_details = {
                'code': cols[0].text.strip(),
                'station_name': cols[1].text.strip(),
                'city': cols[2].text.strip()
            }

            # Add the station details to the city's data
            combined_data['station_data'] = station_details
            break
    return combined_data

# Main function to execute ETL pipeline
if __name__ == "__main__":
    # Scrape city data from Wikipedia
    cities_data = scrape_indian_cities_data(WIKIPEDIA_URL)

    # Scrape latitude and longitude data from latlong.net
    geo_data = []
    for url in LAT_LONG_URL:
        geo_data.extend(scrape_indian_cities_lat_long(url))

    combined_data = combine_data(cities_data, geo_data)

    # Get weather data for each city
    for city, data in combined_data.items():
        lat = data['latitude']
        lon = data['longitude']
        weather_data = get_weather(lat, lon)
        if weather_data:
            data.update(weather_data)


    # Print the combined data
    for city, data in combined_data.items():
        if 'city_id' in data:
            print(f"City ID: {data['city_id']}")
            print(f"City: {city}")
            print(f"Population: {data['population']}")
            print(f"Latitude: {data['latitude']}")
            print(f"Longitude: {data['longitude']}")
            print(f"Temperature: {data['temperature']}")
            print(f"Humidity: {data['humidity']}")
            print(f"Wind Speed: {data['wind_speed']}")
            print(f"Weather Conditions: {data['weather_conditions']}")
            print()

    # Print the length of the combined data
    print(len(combined_data))
