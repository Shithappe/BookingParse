from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

checkin = [datetime.now().date() + timedelta(hours=5) + timedelta(days=i) for i in range(30)]
checkout = [datetime.now().date() + timedelta(hours=5) + timedelta(days=i+1) for i in range(30)]         # tomorrow


print(checkin)
print(checkout)