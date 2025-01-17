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

def load_variables_data(variable_data, forecast_id, model_type, conn, variable_name):
    
    member_value = variable_data["MemberValue"]
    orig_member_value = member_value
    if not member_value:
        member_value = ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"

    df_data_load = pd.DataFrame(columns=["forecast_id", "variable", "value", "model_number"])

    for idx, value in enumerate(member_value.split(",")):
        model = idx+1
        if value == "":
            value = None
        df_data_load.loc[idx] = [forecast_id, variable_name, value, model]
    df_data_load.to_sql("day{0}_forecast_model_values".format(model_type), con=conn, if_exists='append', schema="forecast_data", index=False)

    mean_value = None if variable_data["MeanValue"] == "None" else variable_data["MeanValue"]
    min_value = None if variable_data["MinValue"] == "None" else variable_data["MinValue"]
    max_value = None if variable_data["MaxValue"] == "None" else variable_data["MaxValue"]
    nof_members = variable_data["NoOfMembers"]
    units = variable_data["Unit"]
    
    df_summaries = pd.DataFrame(columns=["forecast_id", "variable", "mean_value", "max_value", "min_value", "nof_members", "units","orig_member_value"])
    df_summaries.loc[0] = [forecast_id, variable_name, mean_value, max_value, min_value, nof_members, units, orig_member_value]
    df_summaries.to_sql("day{0}_forecast_variable_summaries".format(model_type), con=conn, if_exists='append', schema="forecast_data", index=False)

def main():
    folders = { #based on email thread from Andrew Mead, 14th Jan 2025, ALL sites use Cranwell day/night model
       "folder_list": [
            {"name":"CW28", "site":"Cranwell", "model_type": "28"},
            {"name":"BB28", "site":"Brooms Barn", "model_type": "28"},
            {"name":"RR28", "site":"Rothamsted", "model_type": "28"},
            {"name":"LF28", "site":"Leconfield", "model_type": "28"},
            {"name":"TC28", "site":"Topcliffe", "model_type": "28"},
            {"name":"WB28", "site":"Wellesbourne", "model_type": "28"},
            {"name":"WR28", "site":"Writtle", "model_type": "28"},
            {"name":"CW10", "site":"Cranwell", "model_type": "10"},
            {"name":"BB10", "site":"Brooms Barn", "model_type": "10"},
            {"name":"RR10", "site":"Rothamsted", "model_type": "10"},
            {"name":"LF10", "site":"Leconfield", "model_type": "10"},
            {"name":"TC10", "site":"Topcliffe", "model_type": "10"},
            {"name":"WB10", "site":"Wellesbourne", "model_type": "10"},
            {"name":"WR10", "site":"Writtle", "model_type": "10"},
        ]}

    with get_forecast_data_con() as conn:
        # src_dir is the mapped location of the BBRO data on the e-RA Sharepoint. If you are not ostlerr you should update this!
        src_dir = "C:/Users/ostlerr/Rothamsted Research/e-RA - Documents/BBROWeatherQuest/" 
        try:
            for folder in folders["folder_list"]:
                folder_name = folder["name"]
                site  = folder["site"]
                model_type = str(folder["model_type"])
                print("processing folder {0}".format(folder_name))
                
                for filename in os.listdir(src_dir + folder_name):
                    # dataset = json.load(file) # Need to refactor this to below code as powerautomate files include a byte order mark symbol. 
                    # Using utf-8-sig does remove this for strings, but still have a problem if loading json directly from file
                    file = open(os.path.join(src_dir + folder_name,filename),encoding="utf-8-sig")
                    # dataset = json.load(file) # Doesn't work with BOM
                    file_content = file.read()
                    dataset = json.loads(file_content)

                    model_run_id = 0
                    add = False    
                    metadata = dataset["MetaData"]
                
                    node_number = None #0 # dummy value
                    if "NodeNumber" in metadata[0]:
                        node_number = metadata[0]["NodeNumber"]
                    
                    if model_type == "10":
                        model_run = datetime.strptime(metadata[0]["ModelRun"],"%Y-%m-%d")
                        last_updated = metadata[0]["LastUpdated"]

                        df_temp = pd.io.sql.read_sql("select id from forecast_data.day10_model_runs where last_updated = '{0}' and forecast_site = '{1}'".format(last_updated,site),conn)

                        if not df_temp.empty:
                            print("exists")
                        else:
                            add = True
                            print("new {0}".format(last_updated))    
                            df_model_run = pd.DataFrame(columns=["node_number", "model_run", "last_updated", "forecast_site", "source_file"])
                            df_model_run.loc[0] = [node_number, model_run, last_updated, site, filename]
                    else:
                        last_updated = metadata[0]["LastUpdated"]
                        
                        df_temp = pd.io.sql.read_sql("select id from forecast_data.day28_model_runs where last_updated = '{0}' and forecast_site = '{1}'".format(last_updated,site),conn)

                        if not df_temp.empty:
                            print("exists")
                        else:
                            add = True
                            print("new {0}".format(last_updated)) 
                            
                            model = ""
                            if "Model" in metadata[0]:
                                model = metadata[0]["Model"]
                            df_model_run = pd.DataFrame(columns=["node_number", "model", "last_updated", "forecast_site", "source_file"])
                            df_model_run.loc[0] = [node_number, model, last_updated, site, filename]

                    if add:
                        # Get the PK for the new mode        
                        df_model_run.to_sql("day{0}_model_runs".format(model_type), con=conn, if_exists='append', schema="forecast_data", index=False)

                        df_temp = pd.io.sql.read_sql("select id from forecast_data.day{0}_model_runs order by id desc limit 1".format(model_type), conn)
                        
                        model_run_id = df_temp["id"].iloc[0]
                    
                        # add the forecasts
                        data = dataset["Data"]
                        forecast_day = 0
                        old_forecast_date = None
                        for idx, forecast in enumerate(data):
                            forecast_date = forecast["Forecast"]["ValidTime"].split('T')[0]
                            forecast_hour = int(forecast["Forecast"]["ValidTime"].split('T')[1].split(":")[0])
                            if old_forecast_date != forecast_date:
                                old_forecast_date = forecast_date
                                forecast_day = forecast_day + 1
                            
                            df_forecast = pd.DataFrame(columns=["model_run_id", "forecast_date", "forecast_day", "forecast_hour"])
                            df_forecast.loc[0] = [model_run_id, forecast_date, forecast_day, forecast_hour]
                            
                            df_forecast.to_sql("day{0}_forecasts".format(model_type), con=conn, if_exists='append', schema="forecast_data", index=False)

                            df_temp = pd.io.sql.read_sql("select id from forecast_data.day{0}_forecasts order by id desc limit 1".format(model_type), conn)
                            
                            forecast_id = df_temp["id"].iloc[0]

                            load_variables_data(forecast["Forecast"]["Rainfall"], forecast_id, model_type, conn, "rainfall")
                            load_variables_data(forecast["Forecast"]["PAR"], forecast_id, model_type, conn, "PAR")
                            load_variables_data(forecast["Forecast"]["AverageTemperature"], forecast_id, model_type, conn, "avgtemp")
                            load_variables_data(forecast["Forecast"]["MaximumTemperature"], forecast_id, model_type, conn, "maxtemp")
                            load_variables_data(forecast["Forecast"]["MinimumTemperature"], forecast_id, model_type, conn, "mintemp")

        except Exception as e:
            print(f"An error occurred: {e}")
            print(filename)
            print(e.with_traceback())

        finally:
            # Close the cursor and connection
            conn.dispose()
      
if __name__ == '__main__':
    main()