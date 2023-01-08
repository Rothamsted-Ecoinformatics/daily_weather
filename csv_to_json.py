import os
import pandas as pd
import csv
import json

model_run_dates = ("2022-03-09","2022-03-10","2022-03-11","2022-03-12","2022-03-13","2022-03-14","2022-03-15","2022-03-16")
for model_run_date in model_run_dates:
    df = pd.read_csv('N:\BBRO\Weatherquest\daily\csv\d10-Cranwell{0}.csv'.format(model_run_date))
    entries = []
    for index, row in df.iterrows():
        entry = { 
                "Forecast": {
                    "ValidTime" : row["ValidTime"],
                    "Rainfall" : {
                        "MemberValue": row["RainfallMemberValue"],
                        "MeanValue": row["RainfallMean"],
                        "MinValue": row["RainfallMin"],
                        "MaxValue": row["RainfallMax"],
                        "NoOfMembers": 50,
                        "Unit": "mm"
                    },
                    "AverageTemperature": {
                        "MemberValue": row["AverageTemperatureMemberValue"],
                        "MeanValue": row["AverageTemperatureMean"],
                        "MinValue": row["AverageTemperatureMin"],
                        "MaxValue": row["AverageTemperatureMax"],
                        "NoOfMembers": 50,
                        "Unit": "C"
                    },
                    "PAR": {
                        "MemberValue": row["PARMemberValue"],
                        "MeanValue": row["PARMean"],
                        "MinValue": row["PARMin"],
                        "MaxValue": row["PARMax"],
                        "NoOfMembers": 50,
                        "Unit": "umols/m2/s"
                    },
                    "MaximumTemperature": {
                        "MemberValue": row["MaximumTemperatureMemberValue"],
                        "MeanValue": row["MaximumTemperatureMean"],
                        "MinValue": row["MaximumTemperatureMin"],
                        "MaxValue": row["MaximumTemperatureMax"],
                        "NoOfMembers": 50,
                        "Unit": "C"
                    },
                    "MinimumTemperature": {
                        "MemberValue": row["MinimumTemperatureMemberValue"],
                        "MeanValue": row["MinimumTemperatureMean"],
                        "MinValue": row["MinimumTemperatureMin"],
                        "MaxValue": row["MinimumTemperatureMax"],
                        "NoOfMembers": 50,
                        "Unit": "C"
                    }
                }
            }
        

        entries.append(entry)

    # Build the whole file output
    out = {
        "MetaData": [
            {
                "RequestedLatitude": "53.0312",
			    "RequestedLongitude": "-0.5036",
			    "NearestLatitude": "53",
			    "NearestLongitude": "-0.5",
			    "ClosestPoint": "2.161032333239732",
			    "NodeNumber": "2253",
                "ModelRun": model_run_date,
                "LastUpdated": "{0} 07:30:00".format(model_run_date),
                "WindHeight": 10,
                "WindHeightCoefficient": 1
            }
        ],
        "Data": [
            entries
        ]
    }    

    with open("N:/BBRO/Weatherquest/daily/json/CW10/{0}.json".format(model_run_date), "w") as outfile:
        json.dump(out, outfile)