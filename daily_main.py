import os
import json
import sqlite3 as sql
import time
import csv
import requests
from progress.bar import Bar   #ONLY FOR VISUAL

# These set terminal colours
W  = '\033[0m'   
R  = '\033[1;31m'
DR = '\033[0;31m'
G  = '\033[1;32m'
DG = '\033[0;32m'
C  = '\033[1;36m'
V  = '\033[0;35m'
P  = '\033[1;35m' 
B  = '\033[0;36m'

print ()
print (C+">>> API Importation..."+W)
 
try :
    #start_time = time.time()
    #current_time = time.time()
    
    sites = {"BroomsBarn": {"lat":"51.2605","long":"0.5656","Id":"1"},"Cranwell":{"lat":"53.0312","long":"-0.5036","Id":"2"}}
    calls = {
        "d28":{"forecast":"d28","url":"https://api.weatherquest.co.uk/API/Forecast/API-28d-EPS-Forecast?APIKey=9f8e75fe66afab4ad29b09a21bb7865e&APIToken=b358a3afa1e878a49a6bed8fe5a0b629&Lat={0}&Lon={1}&WSUnit=m/s&Vars=MinimumTemperature,MaximumTemperature,AverageTemperature,Rainfall,PAR"},
        "d10":{"forecast":"d10","url":"https://api.weatherquest.co.uk/API/Forecast/API-EPS-Forecast?APIKey=9f8e75fe66afab4ad29b09a21bb7865e&APIToken=b358a3afa1e878a49a6bed8fe5a0b629&Lat={0}&Lon={1}&WSUnit=m/s&Vars=MinimumTemperature,MaximumTemperature,AverageTemperature,Rainfall,PAR"}
    }
    
    for call in calls:
        forecast_type = calls[call]["forecast"]
        for site in sites:
            site_id = sites[site]["Id"]
            resp = requests.get(calls[call]["url"].format(sites[site]['lat'],sites[site]['long']))
            data = resp.json()
            meta = data["MetaData"]
            Up = meta[0]["LastUpdated"].split(" ")[0]
            if call == "d10":
                Run = meta[0]["ModelRun"]
            else:
                Run = meta[0]["Model"]
            Node = meta[0]["NodeNumber"]
            MemberValue = ["MaximumTemperature","MinimumTemperature","AverageTemperature","Rainfall","PAR"]
            forecastData = data["Data"]  
            date = data['Data'][0]['Forecast']['ValidTime'].split('T')[0]
            filename = "json\\EPS-" + calls[call]["forecast"] + "_" + site + "_" + date + ".json"
            with open(filename, 'w') as outfile:
                json.dump(data, outfile) 
            print(B+filename + " created"+W)
    
    '''end_time = time.time()
    total_time = round((end_time - current_time), 1)
    time_string = ''
    if total_time > 60:
        minute = 0
        while total_time > 60:
            minute += 1
            total_time -= 60
        time_string += str(minute) + " mn "
    time_string += str(round(total_time)) + " sec"

    print(V+"API Downloaded in " + time_string+W)  
    '''
    print()
    print(C+'>>> Completing DataBase...'+W)
    
    #current_time = time.time()
    connection = sql.connect("db/CompleteDataBase.db")
    cursor = connection.cursor()
    cursor.execute("""SELECT * FROM Models""")
    model_number = len(cursor.fetchall())
    cursor.execute("""SELECT Forecast_id FROM Forecasts""")
    id_list = cursor.fetchall()
    if id_list == []:
        last_forecast_id = 1
    else:
        last_forecast_id = id_list[-1][0]+1
    cursor.execute("""SELECT Id FROM Model_forecasts""")
    id_list = cursor.fetchall()
    if id_list == []:
        result_id = 1
    else :
        result_id = id_list[-1][0]+1
    
    count = 0
    for file in os.scandir('json'):
        if file.is_file():
            count += 1
    
    bar = Bar(B + 'Processing', max=count)
    to_print = ""
    
    for file in os.scandir('json'):
        if file.is_file():
            path = file.path
            outfile = open(path)
            data = json.load(outfile)
            outfile.close()
            
            folder = path.split('\\')[0]
            filename = path.split('\\')[1]
            forecast_type = filename.split('_')[0].split('-')[1]
            site = filename.split('_')[1]
            Run = filename.split('_')[2].split('.')[0]
            MemberValue = ["MaximumTemperature","MinimumTemperature","AverageTemperature","Rainfall","PAR"]
            forecastData = data["Data"]
            meta = data["MetaData"]
            Up = meta[0]["LastUpdated"].split(" ")[0]
                                
            if site == 'BroomsBarn' :
                site_id = 1
            else : 
                site_id = 2
            
            cursor.execute("""
                           SELECT Forecast_id 
                           FROM Forecasts 
                           WHERE Site_id = ?
                           AND Forecast_type = ?
                           AND Pull_date = ?
                           """, (site_id, forecast_type, Run))
            if (cursor.fetchall() == []):  
                for forecast in forecastData:
                    valid_time_complete = forecast["Forecast"]["ValidTime"].replace('Z','').split('T')
                    valid_date = valid_time_complete[0]
                    valid_hour = int(valid_time_complete[1].split(':')[0])
                    cursor.execute("""
                                   INSERT INTO Forecasts VALUES(?,?,?,?,?,?)
                                   """, (last_forecast_id, site_id, Run, valid_date, valid_hour, forecast_type))
                                                
                    Valid_time = forecast["Forecast"]["ValidTime"]
                    Max_Temp = str(forecast["Forecast"]["MaximumTemperature"]["MemberValue"]).split(",")
                    Min_Temp = str(forecast["Forecast"]["MinimumTemperature"]["MemberValue"]).split(",")
                    Avg_Temp = str(forecast["Forecast"]["AverageTemperature"]["MemberValue"]).split(",")
                    Rainfall = str(forecast["Forecast"]["Rainfall"]["MemberValue"]).split(",")
                    PAR = str(forecast["Forecast"]["PAR"]["MemberValue"]).split(",")
                    if len(Max_Temp) == 1:
                        Max_Temp = ['']*model_number
                    if len(Min_Temp) == 1:
                        Min_Temp = ['']*model_number
                    for member_list in [Max_Temp, Min_Temp, Avg_Temp, Rainfall, PAR]:
                        for model in range(0,model_number):
                            if member_list[model] == '':
                                member_list[model] = None
                            else:
                                member_list[model] = float(member_list[model])
                        
                    for model in range(0,model_number):
                        cursor.execute("""
                                       INSERT INTO Model_forecasts VALUES(?,?,?,?,?,?,?,?)
                                       """, (result_id, last_forecast_id, model+1, Max_Temp[model], Min_Temp[model], Avg_Temp[model], Rainfall[model], PAR[model]))
                        result_id+=1
                    last_forecast_id+=1
                import_delay = round((time.time() - current_time), 1)
                time_string = ''
                
                to_print += B+path + ' completed'+W + '\n'
            
            for Parameter in MemberValue:
                csvname = "csv\\" + forecast_type + "_" + site + "_" + Run + "_" + Parameter + ".csv"
                with open(csvname, mode='w', newline='') as f:
                    w = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    titles=["Site","PullDate","ValidDate","ValidHoure","Mean","Min","Max"]
                    for model in range(0,model_number):
                        string="model"+str(model+1)
                        titles+=[string]  
                    titles.append('average')
                    w.writerow(titles)
                    for rec in forecastData:
                        fc = rec["Forecast"]
                        valueList = str(fc[Parameter]["MemberValue"]).split(",")
                        toWrite = [site,Run,fc["ValidTime"].split('T')[0],fc["ValidTime"].split('T')[1].replace('Z',''),
                        str(fc[Parameter]["MeanValue"]),
                        str(fc[Parameter]["MinValue"]),
                        str(fc[Parameter]["MaxValue"])]
                        for value in valueList:
                            toWrite+=[value]
                        average = 0
                        divider = 0
                        for value in valueList:
                            if value != 'None' and value != '':
                                average += float(value)
                                divider += 1
                        if divider != 0:
                            toWrite.append(str(round(average/divider, 1)))
                        else:
                            toWrite.append('None')
                        
                        w.writerow(toWrite)
                    f.close()
            connection.commit()
        bar.next()
    bar.finish()
    
    print(to_print)
            
    end_time = time.time()
    total_time = round((end_time - current_time), 1)
    time_string = ''
    if total_time > 60:
        minute = 0
        while total_time > 60:
            minute += 1
            total_time -= 60
        time_string += str(minute) + " mn "
    time_string += str(round(total_time)) + " sec"

    print(V+"Data imported in " + time_string+W)  

    print()
    print(C+'>>> Updating Max/Min...'+W)
    
    current_time = time.time()

    ModelNumber = 50

    for Site in ['BroomsBarn', 'Cranwell']:
        for ForecastType in ['d10', 'd28']:
            Maxdata = []
            Mindata = []
                        
            if Site == 'Cranwell':
                MetaData = {'SiteName': Site, 
                            'ForecastType': ForecastType, 
                            "RequestedLatitude": "53.0312",
                            "RequestedLongitude": "-0.5036",
                            "NearestLatitude": "53",
                            "NearestLongitude": "-0.5",
                            "ClosestPoint": "2.161032333239732"}
                cursor.execute("""
                                SELECT maxt.Pull_date, maxt.Valid_date, maxt.Model_id, Max_Temp, Min_Temp FROM
                                (SELECT Forecast_type, Pull_date, Valid_date, Model_id, MIN(Minimum_temp) as Min_Temp FROM Forecasts JOIN Model_forecasts
                                ON Model_forecasts.Forecast_id = Forecasts.Forecast_id
                                WHERE Site_id = 2
                                AND Forecast_type = ?
                                AND ((Valid_hour >= 0 AND Valid_hour <= 6) OR (Valid_hour >= 21))
                                GROUP BY Forecast_type, Pull_date, Valid_date, Model_id) maxt
                                INNER JOIN
                                (SELECT Forecast_type, Pull_date, Valid_date, Model_id, MAX(Maximum_temp) as Max_Temp FROM Forecasts JOIN Model_forecasts
                                ON Model_forecasts.Forecast_id = Forecasts.Forecast_id
                                WHERE Site_id = 2
                                AND Forecast_type = ?
                                AND (Valid_hour >= 9) AND (Valid_hour <= 18)
                                GROUP BY Forecast_type, Pull_date, Valid_date, Model_id) mint
                                ON maxt.Forecast_type = mint. Forecast_type 
                                AND maxt.Pull_date = mint.Pull_date
                                AND maxt.Valid_date = mint.Valid_date
                                AND maxt.Model_id = mint.Model_id
                               """, (ForecastType, ForecastType))
                result = cursor.fetchall()
            
            elif Site == 'BroomsBarn':
                MetaData = {'SiteName': Site, 
                            'ForecastType': ForecastType, 
                            "RequestedLatitude": "51.2605",
                            "RequestedLongitude": "0.5656",
                            "NearestLatitude": "51.5",
                            "NearestLongitude": "0.5",
                            "ClosestPoint": "16.788958792063735"}
                cursor.execute("""
                                SELECT maxt.Pull_date, maxt.Valid_date, maxt.Model_id, Max_Temp, Min_Temp FROM
                                (SELECT Forecast_type, Pull_date, Valid_date, Model_id, MIN(Minimum_temp) as Min_Temp FROM Forecasts JOIN Model_forecasts
                                ON Model_forecasts.Forecast_id = Forecasts.Forecast_id
                                WHERE Site_id = 1
                                AND Forecast_type = ?
                                AND (Valid_hour >= 0) AND (Valid_hour >= 9)
                                GROUP BY Forecast_type, Pull_date, Valid_date, Model_id) maxt
                                INNER JOIN
                                (SELECT Forecast_type, Pull_date, Valid_date, Model_id, MAX(Maximum_temp) as Max_Temp FROM Forecasts JOIN Model_forecasts
                                ON Model_forecasts.Forecast_id = Forecasts.Forecast_id
                                WHERE Site_id = 1
                                AND Forecast_type = ?
                                AND (Valid_hour >= 9) AND (Valid_hour <= 21)
                                GROUP BY Forecast_type, Pull_date, Valid_date, Model_id) mint
                                ON maxt.Forecast_type = mint. Forecast_type 
                                AND maxt.Pull_date = mint.Pull_date
                                AND maxt.Valid_date = mint.Valid_date
                                AND maxt.Model_id = mint.Model_id
                               """, (ForecastType, ForecastType))
                result = cursor.fetchall()
                
            for row in result:
                if Maxdata == []:
                    Maxdata.append({'PullDate': row[0], 'Forecasts': []})
                else:
                    if row[0] != Maxdata[-1]['PullDate']:
                        Maxdata.append({'PullDate': row[0], 'Forecasts': []})
            
                if Maxdata[-1]['Forecasts'] == []:
                    Maxdata[-1]['Forecasts'].append({'ValidDate': row[1], 'Values': ''})
                else :
                    if row[1] != Maxdata[-1]['Forecasts'][-1]['ValidDate']:
                        Maxdata[-1]['Forecasts'].append({'ValidDate': row[1], 'Values': ''})
                
                if len(Maxdata[-1]['Forecasts'][-1]['Values'].split(',')) == ModelNumber:
                    Maxdata[-1]['Forecasts'][-1]['Values'] += str(row[3])
                else:
                    Maxdata[-1]['Forecasts'][-1]['Values'] += str(row[3]) + ','
                
                if Mindata == []:
                    Mindata.append({'PullDate': row[0], 'Forecasts': []})
                else:
                    if row[0] != Mindata[-1]['PullDate']:
                        Mindata.append({'PullDate': row[0], 'Forecasts': []})
            
                if Mindata[-1]['Forecasts'] == []:
                    Mindata[-1]['Forecasts'].append({'ValidDate': row[1], 'Values': ''})
                else :
                    if row[1] != Mindata[-1]['Forecasts'][-1]['ValidDate']:
                        Mindata[-1]['Forecasts'].append({'ValidDate': row[1], 'Values': ''})
                
                if len(Mindata[-1]['Forecasts'][-1]['Values'].split(',')) == ModelNumber:
                    Mindata[-1]['Forecasts'][-1]['Values'] += str(row[4])
                else:
                    Mindata[-1]['Forecasts'][-1]['Values'] += str(row[4]) + ','
                
            Maxwriter = {'MetaData': MetaData, 'Data': Maxdata}
            Minwriter = {'MetaData': MetaData, 'Data': Mindata}
                
            with open(('maxmin_json/' + ForecastType + '_' + Site + '_Maximum.json'), 'w') as outfile:
                json.dump(Maxwriter, outfile) 
                outfile.close()
            
            with open(('maxmin_json/' + ForecastType + '_' + Site + '_Minimum.json'), 'w') as outfile:
                json.dump(Minwriter, outfile) 
                outfile.close()
            
            with open(('maxmin_csv/' + ForecastType + '_' + Site + '_Maximum.csv'), mode='w', newline='') as f:
                print(B+('maxmin_csv/' + ForecastType + '_' + Site + '_Maximum.csv updated')+W)
                w = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                titles=["Site","PullDate","ValidDate"]
                for model in range(0,ModelNumber):
                    string="model"+str(model+1)
                    titles+=[string]  
                titles.append('average')
                w.writerow(titles)
                for pulldate in Maxdata:
                    for forecast in pulldate['Forecasts']: 
                        values = forecast['Values'].split(',')
                        towrite = [Site, pulldate['PullDate'], forecast['ValidDate']]
                        average = 0
                        divider = 0
                        for value in values:
                            towrite.append(value)
                            if value != 'None':
                                average += float(value)
                                divider += 1
                        towrite.append(str(round(average/divider, 1)))
                        w.writerow(towrite)
                f.close()
            
            with open(('maxmin_csv/' + ForecastType + '_' + Site + '_Minimum.csv'), mode='w', newline='') as f:
                print(B+('maxmin_csv/' + ForecastType + '_' + Site + '_Minimum.csv updated')+W)
                w = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                titles=["Site","PullDate","ValidDate"]
                for model in range(0,ModelNumber):
                    string="model"+str(model+1)
                    titles+=[string]  
                titles.append('average')
                w.writerow(titles)
                for pulldate in Mindata:
                    for forecast in pulldate['Forecasts']: 
                        values = forecast['Values'].split(',')
                        towrite = [Site, pulldate['PullDate'], forecast['ValidDate']]
                        average = 0
                        divider = 0
                        for value in values:
                            towrite.append(value)
                            if value != 'None':
                                average += float(value)
                                divider += 1
                        towrite.append(str(round(average/divider, 1)))
                        w.writerow(towrite)
                f.close()

    end_time = time.time()
    total_time = round((end_time - current_time), 1)
    time_string = ''
    if total_time > 60:
        minute = 0
        while total_time > 60:
            minute += 1
            total_time -= 60
        time_string += str(minute) + " mn "
    time_string += str(round(total_time)) + " sec"

    print(V+"Max/Min updated in " + time_string+W)  
    print()
    
    total_time = round((end_time - start_time), 1)
    time_string = ''
    if total_time > 60:
        minute = 0
        while total_time > 60:
            minute += 1
            total_time -= 60
        time_string += str(minute) + " mn "
    time_string += str(round(total_time)) + " sec"

    print(P+"Process ended in " + time_string+W)  
            
except Exception as e:
    print (R, "[ERROR]", e, W)
    connection.rollback()
        
finally :
    connection.close()