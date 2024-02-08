import requests

url = "https://rstations.p.rapidapi.com/"

payload = { "search": "Bangalore" }
headers = {
	"content-type": "application/json",
	"Content-Type": "application/json",
	"X-RapidAPI-Key": "38096c9dc7mshfade25f5f2430fep1779a0jsn0cb6a958622c",
	"X-RapidAPI-Host": "rstations.p.rapidapi.com"
}

response = requests.post(url, json=payload, headers=headers)

print(response.json())