import requests
import json


url = "https://booking-com.p.rapidapi.com/v1/static/regions"

querystring = {"page":"0","country":"th"}

headers = {
	"x-rapidapi-key": "8055d9b3d1msh4deb132b9d59203p1421c4jsnd67afd362fe3",
	"x-rapidapi-host": "booking-com.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

# print(response.json())

# for region in response.json()['result']:
#     print(region['region_id'])

data = response.json()

with open('regions.txt', 'w') as file:
    # Итерируем по полю "result"
    if 'result' in data:
        for region in data['result']:
            # Записываем значение "region_id" в файл
            file.write(f"{region['region_id']}\n")
    else:
        print("Поле 'result' не найдено в ответе")