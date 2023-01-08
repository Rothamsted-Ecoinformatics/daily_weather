import os
import json
import sqlite3
from datetime import datetime, timedelta
import pandas as pd

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    os.remove(db_file)    
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def insert_model_run(conn, params):
    sql = "insert into model_runs (node_number, model_run_date, model_type, site, model_run_day) values (?,?,?,?,?)"
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode = OFF")
    cur.execute("PRAGMA locking_mode = EXCLUSIVE")
    cur.execute(sql, params)
    #conn.commit()
    return cur.lastrowid

def insert_forecasts(conn, params):
    sql = "insert into forecasts (model_run_id, forecast_date, forecast_day, day_period, forecast_hour, include) values (?,?,?,?,?,?)"
    cur = conn.cursor()
    # want to set some prgamas to improve insert performance
    cur.execute("PRAGMA journal_mode = OFF")
    cur.execute("PRAGMA locking_mode = EXCLUSIVE")
    cur.execute(sql, params)
    #conn.commit()
    return cur.lastrowid

def insert_forecast_model_values(conn, forecast_id, variable, values):
    if values:
        sql = "insert into forecast_model_values (forecast_id, variable, value, model) values (?,?,?,?)"
        parameters = []
        for model, value in enumerate(values.split(","), start=1):
            if value == "":
                value = 0
            parameters.append((forecast_id, variable, value, model))
        
        cur = conn.cursor()
        # want to set some prgamas to improve insert performance
        cur.execute("PRAGMA journal_mode = OFF")
        cur.execute("PRAGMA locking_mode = EXCLUSIVE")
        cur.executemany(sql, parameters)

        # this is very slow - using executemany above is much faster    
        #df = pd.DataFrame(columns = ["forecast_id", "variable", "value","model"])
        #for model, value in enumerate(values.split(","), start=1):
        #    df = df.append({'forecast_id':forecast_id, 'variable':variable, 'value': value, 'model': model}, ignore_index=True)
        #df.to_sql("forecast_model_values", conn, if_exists="append", index=False, method="multi")
        #conn.commit()

def main():
    conn = create_connection(r"N:\BBRO\Weatherquest\WeatherQuestDB.db")

    # model run id = the model run date expressed as an int of YYYYMMDD 
    model_runs_sql = """CREATE TABLE IF NOT EXISTS model_runs (
                        id integer PRIMARY KEY,
                        node_number integer,
                        model_run_date text,
                        model_type integer,
                        model_run_day text,
                        site text
                    ); """

    # forecast id =  the model run date, forecast date and hour expressed as an int of YYYYMMDDYYMMDDHH
    forecasts_sql = """CREATE TABLE IF NOT EXISTS forecasts (
                        id integer PRIMARY KEY,
                        model_run_id integer,
                        forecast_date text,
                        forecast_day text,
                        day_period text,
                        forecast_hour text,
                        include integer
                    ); """

    forecast_model_values_sql = """CREATE TABLE IF NOT EXISTS forecast_model_values (
                        id integer PRIMARY KEY,
                        forecast_id integer,
                        variable text,
                        value real,
                        model integer
                    ); """


    create_table(conn, model_runs_sql)
    create_table(conn, forecasts_sql)
    create_table(conn, forecast_model_values_sql)

    folders = {
        "folder_list": [
            {"name":"CW28", "site":"Cranwell", "model_type": "28", "night":['21', '00', '03', '06']},
            {"name":"BB28", "site":"Cranwell", "model_type": "28", "night":['00', '03', '06', '09']},
            {"name":"CW10", "site":"Cranwell", "model_type": "10", "night":['21', '00', '03', '06']},
            {"name":"BB10", "site":"Brooms Barn", "model_type": "10", "night":['00', '03', '06', '09']}
        ]}
    
    for folder in folders["folder_list"]:
        folder_name = folder["name"]
        print("processing folder {0}".format(folder_name))
        site  = folder["site"]
        model_type = folder["model_type"]
        night = folder["night"]

        for filename in os.listdir("N:\BBRO\Weatherquest\daily\json\\" + folder_name):
            # dataset = json.load(file) # Need to refactor this to below code as powerautomate files include a byte order mark symbol. 
            # Using utf-8-sig does remove this for strings, but still have a problem if loading json directly from file
            file = open(os.path.join("N:\BBRO\Weatherquest\daily\json\\" + folder_name,filename),encoding="utf-8-sig") #
            print("processing file {0}".format(filename))
            # dataset = json.load(file) # Doesn't work with BOM
            file_content = file.read()
            dataset = json.loads(file_content)

            metadata = dataset["MetaData"]
            node_number = ""
            if "NodeNumber" in metadata:
                node_number = metadata[0]["NodeNumber"]

            if model_type == "10":
                model_run = metadata[0]["ModelRun"]
                model_run_date = datetime.strptime(model_run,"%Y-%m-%d")
                model_run_day = model_run_date.strftime('%A')[:3]
            else:
                model_run = metadata[0]["LastUpdated"].split(' ')[0]
                if "Model" in metadata:
                    model_run_day = metadata[0]["Model"]
                else: # For when model is missing because from earlier data files.
                    model_run_date = datetime.strptime(model_run,"%Y-%m-%d")
                    model_run_day = model_run_date.strftime('%A')[:3] 

            # add the model run to the database
            model_run_id = insert_model_run(conn, (node_number, model_run, model_type, site, model_run_day))

            # add the forecasts
            data = dataset["Data"]
            # 1. Need a rule to flag forecasts for inclusion or not. The first 7 and last should be ignored
            # 2. Need to flag the forecast day 
            # 3. Need to flag the forecast day period (day or night)
            nof_records = len(data)
            print("nof_records = {0}".format(nof_records))
            for idx, forecast in enumerate(data):
                include = 1
                if model_type == "10" and (idx < 7 or idx == nof_records-1):
                    include = 0
                
                forecast_date = forecast["Forecast"]["ValidTime"].split('T')[0]
                
                forecast_hour = forecast["Forecast"]["ValidTime"].split('T')[1].split(":")[0]
                
                forecast_day = forecast_date 
                if site == "Cranwell" and forecast_hour == '21':
                    forecast_day_date = datetime.strptime(forecast_date,"%Y-%m-%d")
                    forecast_day_date = forecast_day_date + timedelta(days=1)     
                    forecast_day = forecast_day_date.strftime("%Y-%m-%d")

                day_period = 'N' if forecast_hour in night else 'D'

                forecast_id = insert_forecasts(conn, (model_run_id, forecast_date, forecast_day, day_period, forecast_hour, include))
                
                # add the model data
                insert_forecast_model_values(conn, forecast_id, "rainfall", forecast["Forecast"]["Rainfall"]["MemberValue"])
                insert_forecast_model_values(conn, forecast_id, "PAR", forecast["Forecast"]["PAR"]["MemberValue"])
                insert_forecast_model_values(conn, forecast_id, "avgtemp", forecast["Forecast"]["AverageTemperature"]["MemberValue"])
                insert_forecast_model_values(conn, forecast_id, "maxtemp", forecast["Forecast"]["MaximumTemperature"]["MemberValue"])
                insert_forecast_model_values(conn, forecast_id, "mintemp", forecast["Forecast"]["MinimumTemperature"]["MemberValue"])
                conn.commit()

if __name__ == '__main__':
    main()