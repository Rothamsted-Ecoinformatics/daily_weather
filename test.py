import os
import json
from datetime import datetime, timedelta

d = "2022-01-27"
forecast_day_date = datetime.strptime(d,"%Y-%m-%d")
dp = forecast_day_date.strftime('%A')[:3]
print(dp)

'''
file = open(os.path.join("N:\BBRO\Weatherquest\daily\json\BB10","2022-09-06.json"),encoding="utf-8-sig")
data = file.read()
print(data[0:100])
#dataset = json.loads(data.encode("utf-8"))
#print(dataset)
if file == None or file == '':
    print("empty")
else:
    dataset = json.loads(data)

print()
#file = str(file.read()).strip("'<>() ").replace('\'', '\"')

#dataset = json.loads(file.read().decode("utf-8"))
print(dataset)
'''