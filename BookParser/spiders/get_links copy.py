import scrapy
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse


class BookingSpider(scrapy.Spider):

    next_month = datetime.now().date() + relativedelta(months=1)

    checkin = next_month
    checkout = next_month + relativedelta(days=1)

    name = "get_links_copy"
    allowed_domains = ["www.booking.com"]
    start_urls = [f"https://www.booking.com/searchresults.en-gb.html?ss=Bali%2C+Indonesia&label=gen173nr-1FCAQoggJCC3NlYXJjaF9iYWxpSAlYBGjpAYgBAZgBCbgBF8gBDNgBAegBAfgBA4gCAagCA7gC3rbaqgbAAgHSAiRmOWE1NjFkOC00ZDM1LTQxMmYtOTAyOS0yYzE5MTYzNWEyZGPYAgXgAgE&lang=en-gb&sb=1&src_elem=sb&src=index&dest_id=835&dest_type=region&search_selected=true&checkin={checkin}&checkout={checkout}&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure"]
    page_count = 1000 #страниц по 25 штук каждая 

    def start_requests(self):
        for i in range(self.page_count):
            count_item = i * 25 + 1
            next_page_url = f"https://www.booking.com/searchresults.en-gb.html?ss=Bali&ssne=Bali&ssne_untouched=Bali&offset={count_item}"
            yield scrapy.Request(url=next_page_url, callback=self.parse)

    def parse(self, response):
        for a_tag in response.css('a[data-testid="title-link"]'):
            link = self.fomat_link(a_tag.css('::attr(href)').extract_first())
            with open('booking_links.txt', 'a', encoding='utf-8') as f:
                f.write(link + '\n')

    def fomat_link(self, link):
        url = urlparse(link)
        query_parameters = parse_qs(url.query)

        query_parameters['group_adults'] = 1
        query_parameters['no_rooms'] = 1
        query_parameters['group_children'] = 0

        query_parameters['checkin'] = self.checkin
        query_parameters['checkout'] = self.checkout

        query_parameters['selected_currency'] = 'USD'

        url = url._replace(query=urlencode(query_parameters, doseq=True))
        return urlunparse(url)
