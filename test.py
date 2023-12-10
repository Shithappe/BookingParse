from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

checkin_today = datetime.now().date() + timedelta(hours=5)  # + relativedelta(days=1)  
checkout_today = checkin_today + relativedelta(days=1)          

checkin_tomorrow = datetime.now().date() + timedelta(hours=5) + relativedelta(days=1)   
checkout_tomorrow = checkin_tomorrow + relativedelta(days=1)

checkin = [datetime.now().date() + timedelta(hours=5) + timedelta(days=i) for i in range(30)]
checkout = [datetime.now().date() + timedelta(hours=5) + timedelta(days=i+1) for i in range(30)]         # tomorrow


# print(checkin)
# print(checkout)

print(datetime.now().date())
