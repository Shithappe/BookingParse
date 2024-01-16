from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

checkin = (datetime.now() + timedelta(hours=5)).date()
checkout = (datetime.now() + timedelta(hours=5) + timedelta(days=1)).date()


print(checkin)
print(checkout)