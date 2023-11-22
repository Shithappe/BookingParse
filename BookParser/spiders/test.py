import scrapy

class MySpider(scrapy.Spider):
    name = 'test'
    start_urls = ['https://www.booking.com/hotel/id/kelingking-tatakan-bungalow.en-gb.html?aid=304142&label=gen173nr-1FCAQoggJCC3NlYXJjaF9iYWxpSAlYBGjpAYgBAZgBCbgBF8gBDNgBAegBAfgBA4gCAagCA7gCsdvaqgbAAgHSAiQ5ODNhMzg2ZC02NzJlLTRlOTgtODQxNi05NmIxYTgxZjU1MznYAgXgAgE&ucfs=1&arphpl=1&group_adults=1&req_adults=2&no_rooms=1&group_children=0&req_children=0&hpos=1&hapos=1&sr_order=popularity&srpvid=fc7a0058d0cb01f7&srepoch=1700179378&from_sustainable_property_sr=1&from=searchresults&checkin=2023-12-17&checkout=2023-12-18#hotelTmpl']  # Замените на свой URL

    def parse(self, response):
        rows = response.css('table tr')
        room_types = []
        prices = []
        current_room_type = ''
        current_prices = []

        for row in rows:
            room_type_cell = row.css('td[ rowspan]::text').get()
            if room_type_cell:
                # Если мы обнаруживаем новый тип номера, сохраняем данные о предыдущем
                if current_room_type:
                    room_types.append(current_room_type)
                    prices.append(current_prices)
                current_room_type = room_type_cell.strip()
                current_prices = []
            price_cell = row.css('td:not([rowspan])::text').get()
            if price_cell:
                current_prices.append(price_cell.strip())

        # Добавляем данные для последнего типа номера
        if current_room_type:
            room_types.append(current_room_type)
            prices.append(current_prices)

        for i, room_type in enumerate(room_types):
            yield {
                'Room Type': room_type,
                'Prices': prices[i]
            }
