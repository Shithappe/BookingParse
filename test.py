from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

checkin = datetime.now().date() + timedelta(hours=5)
checkout = datetime.now().date() + timedelta(hours=5)+timedelta(days=1)

# print(checkin)
# print(checkout)

print(datetime.now().date() + timedelta(hours=5))
print(datetime.now().date() + timedelta(hours=5)+timedelta(days=1))