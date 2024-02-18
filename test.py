from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

checkin = [datetime.now().date() + timedelta(hours=5) + timedelta(weeks=i) for i in range(1, 4)]
checkout = [checkin[i] + timedelta(days=1) for i in range(len(checkin))]

print(checkin)
print(checkout)