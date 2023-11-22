from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

next_month = datetime.now().date() + relativedelta(months=1)

checkin = next_month
checkout = next_month + relativedelta(days=1)

start_urls = [f"https://www.booking.com/searchresults.en-gb.html?ss=Bali&ssne=Bali&ssne_untouched=Bali&checkin={checkin}&checkout={checkout}&group_adults=1&no_rooms=1&group_children=0"]


print(start_urls)