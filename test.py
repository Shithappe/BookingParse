from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

checkin = [datetime.now().date() + timedelta(hours=5) + timedelta(days=i) for i in range(2)]


print(checkin)

