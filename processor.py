import os
import json
import contextlib
import pandas as pd
import sqlalchemy as sa
from datetime import datetime, timedelta
from configparser import ConfigParser

cfg = ConfigParser()
cfg.read('config.ini')

@contextlib.contextmanager
def get_forecast_data_con():
    print(cfg.get('FORECAST_DATA','database'))
    
    con = sa.create_engine("postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(cfg.get('FORECAST_DATA','username'),cfg.get('FORECAST_DATA','password'),cfg.get('FORECAST_DATA','host'),cfg.get('FORECAST_DATA','port'),cfg.get('FORECAST_DATA','database')))

    try:
        yield con
    finally:
        con.dispose()

def main():
    folders = {
        "folder_list": [
            {"name":"CW28", "site":"Cranwell", "model_type": "28", "night":['21', '00', '03', '06']},
            {"name":"BB28", "site":"Brooms Barn", "model_type": "28", "night":['00', '03', '06', '09']},
            {"name":"CW10", "site":"Cranwell", "model_type": "10", "night":['21', '00', '03', '06']},
            {"name":"BB10", "site":"Brooms Barn", "model_type": "10", "night":['00', '03', '06', '09']}
        ]}

    model_run_sql = "insert into forecast_data.model_runs (node_number, model_run_date, model_type, forecast_site, model_run_day) values ({0},'{1}',{2},'{3}','{4}')"
    forecast_sql = "insert into forecast_data.forecasts (model_run_id, forecast_date, forecast_day, day_period, forecast_hour, to_include) values ({0},'{1}','{2}','{3}','{4}',{5})"

    with get_forecast_data_con() as conn:
        # src_dir is the mapped location of the BBRO data on the e-RA Sharepoint. If you are not ostlerr you should update this!
        src_dir = "C:/Users/ostlerr/Rothamsted Research/e-RA - Documents/BBROWeatherQuest/" 
        try:
            for folder in folders["folder_list"]:
                folder_name = folder["name"]
                site  = folder["site"]
                model_type = folder["model_type"]
                night = folder["night"]
                print("processing folder {0}".format(folder_name))
                print(model_type)
                #N 

                for filename in os.listdir(src_dir + folder_name):
                    # dataset = json.load(file) # Need to refactor this to below code as powerautomate files include a byte order mark symbol. 
                    # Using utf-8-sig does remove this for strings, but still have a problem if loading json directly from file
                    file = open(os.path.join(src_dir + folder_name,filename),encoding="utf-8-sig")
                    # dataset = json.load(file) # Doesn't work with BOM
                    file_content = file.read()
                    dataset = json.loads(file_content)

                    metadata = dataset["MetaData"]
                    model_run_date = None
                    node_number = 0 # dummy value
                    if "NodeNumber" in metadata[0]:
                        node_number = metadata[0]["NodeNumber"]

                    if model_type == "10":
                        model_run = metadata[0]["ModelRun"]
                        model_run_date = datetime.strptime(model_run,"%Y-%m-%d")
                        model_run_day_inferred = model_run_date.strftime('%A')[:3]
                        model_run_day_metadata_actual = None
                        if "Model" in metadata[0]:
                            model_run_day_inferred = metadata[0]["ModelRun"]
                            
                    else:
                        model_run = metadata[0]["LastUpdated"].split(' ')[0]
                        model_run_date = datetime.strptime(model_run,"%Y-%m-%d")
                        model_run_day_inferred = model_run_date.strftime('%A')[:3] 
                        model_run_day_metadata_actual = None
                        if "Model" in metadata[0]: 
                            model_run_day_metadata_actual = metadata[0]["Model"]
                        
                    df_temp = pd.io.sql.read_sql("select id from forecast_data.model_runs where model_run_date = '{0}' and model_type = {1} and forecast_site = '{2}'".format(model_run_date,model_type,site),conn)    

                    if not df_temp.empty:
                        print("exists")
                    else:
                        print("new")    
                        df_model_run = pd.DataFrame(columns=["node_number", "model_run_date", "model_type", "forecast_site", "model_run_day_inferred","model_run_day_metadata_actual","source_file"])
                        df_model_run.loc[0] = [node_number, model_run_date, model_type, site, model_run_day_inferred,model_run_day_metadata_actual,filename]
                        
                        df_model_run.to_sql("model_runs", con=conn, if_exists='append', schema="forecast_data", index=False)

                        df_temp = pd.io.sql.read_sql("select id from forecast_data.model_runs order by id desc limit 1", conn)
                        
                        model_run_id = df_temp["id"].iloc[0]
                        
                        # add the forecasts
                        data = dataset["Data"]
                        # 1. Need a rule to flag forecasts for inclusion or not. The first 7 and last should be ignored
                        # This has changed because since April 2022 full days are now being pulled from the day before the actual date.
                        # 2. Need to flag the forecast day 
                        # 3. Need to flag the forecast day period (day or night)
                        nof_records = len(data)
                        for idx, forecast in enumerate(data):
                            include = 1
                            #if site == "Cranwell" and (idx < 7 or idx == nof_records-1): # Applied rule to both sites based on feedback from AM, 23/02/2023
                            if model_type == "10" and (idx < 3 or idx == nof_records-1):
                                include = 0
                            elif model_type == "28" and (idx < 1 or idx == nof_records-1):
                                include = 0
                            
                            forecast_date = forecast["Forecast"]["ValidTime"].split('T')[0]
                            
                            forecast_hour = forecast["Forecast"]["ValidTime"].split('T')[1].split(":")[0]
                            
                            forecast_day = forecast_date 
                            if site == "Cranwell" and forecast_hour == '21':
                                forecast_day_date = datetime.strptime(forecast_date,"%Y-%m-%d")
                                forecast_day_date = forecast_day_date + timedelta(days=1)     
                                forecast_day = forecast_day_date.strftime("%Y-%m-%d")

                            day_period = 'N' if forecast_hour in night else 'D'
                        
                            df_forecast = pd.DataFrame(columns=["model_run_id", "forecast_date", "forecast_day", "day_period", "forecast_hour", "to_include"])
                            df_forecast.loc[0] = [model_run_id, forecast_date, forecast_day, day_period, forecast_hour, include]
                            
                            df_forecast.to_sql("forecasts", con=conn, if_exists='append', schema="forecast_data", index=False)

                            df_temp = pd.io.sql.read_sql("select id from forecast_data.forecasts order by id desc limit 1", conn)
                            forecast_id = df_temp["id"].iloc[0]

                            # add the model data
                            df_load = pd.DataFrame(columns=["forecast_id", "variable", "value", "model"])
                            for idx, value in enumerate(forecast["Forecast"]["Rainfall"]["MemberValue"].split(",")):
                                model = idx+1
                                if value == "":
                                    value = None
                                df_load.loc[idx] = [forecast_id, "Rainfall", value, model]
                            df_load.to_sql("forecast_model_values", con=conn, if_exists='append', schema="forecast_data", index=False)

                            memberValue = forecast["Forecast"]["PAR"]["MemberValue"]
                            if not memberValue:
                                memberValue = ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"

                            for idx, value in enumerate(memberValue.split(",")):
                                model = idx+1
                                if value == "":
                                    value = None
                                df_load.loc[idx] = [forecast_id, "PAR", value, model]
                            df_load.to_sql("forecast_model_values", con=conn, if_exists='append', schema="forecast_data", index=False)

                            memberValue = forecast["Forecast"]["AverageTemperature"]["MemberValue"]
                            if not memberValue:
                                memberValue = ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"

                            for idx, value in enumerate(memberValue.split(",")):
                                model = idx+1
                                if value == "":
                                    value = None
                                df_load.loc[idx] = [forecast_id, "avgtemp", value, model]
                            df_load.to_sql("forecast_model_values", con=conn, if_exists='append', schema="forecast_data", index=False)

                            memberValue = forecast["Forecast"]["MaximumTemperature"]["MemberValue"]
                            if not memberValue:
                                memberValue = ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
                            
                            for idx, value in enumerate(memberValue.split(",")):
                                model = idx+1
                                if value == "":
                                    value = None
                                df_load.loc[idx] = [forecast_id, "maxtemp", value, model]
                            df_load.to_sql("forecast_model_values", con=conn, if_exists='append', schema="forecast_data", index=False)

                            memberValue = forecast["Forecast"]["MinimumTemperature"]["MemberValue"]
                            if not memberValue:
                                memberValue = ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"

                            for idx, value in enumerate(memberValue.split(",")):
                                model = idx+1
                                if value == "":
                                    value = None
                                df_load.loc[idx] = [forecast_id, "mintemp", value, model]                            
                            df_load.to_sql("forecast_model_values", con=conn, if_exists='append', schema="forecast_data", index=False)

        except Exception as e:
            print(f"An error occurred: {e}")
            print(filename)
            print(e.with_traceback())

        finally:
            # Close the cursor and connection
            conn.dispose()
      
if __name__ == '__main__':
    main()