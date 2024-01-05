import scrapy

class PropertySpider(scrapy.Spider):
    name = 'test_spider'
    start_urls = ['https://www.booking.com/hotel/id/villa-miami-new-luxury-7bdr-prime-location.en-gb.html']

    def parse(self, response):

        selected_elements = response.xpath('//*[@id="basiclayout"]/div[1]/div[2]/div/div/div/div/div/ul/li/div[2]/div')

        # Обработка найденных элементов
        for element in selected_elements:
            # Извлечение текста из каждого элемента и вывод его в консоль
            selected_text = element.xpath('./text()').get()
            print(selected_text) 
