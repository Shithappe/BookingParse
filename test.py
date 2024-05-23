from datetime import datetime, timedelta

class MyClass:
    def __init__(self):
        self.today = datetime.now().date()
        self.checkin = [self.today + timedelta(hours=5) + timedelta(weeks=i) for i in range(1, 4)]
    
    def add_week_to_last_checkin(self):
        if self.checkin:
            new_date = [self.checkin[-1] + timedelta(days=i) for i in range(1, 8)]
            # self.checkin.append(new_date)
            print(new_date)

# Пример использования
obj = MyClass()
print("Before:", obj.checkin)
obj.add_week_to_last_checkin()
# print("After:", obj.checkin)
