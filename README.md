# Overview
This script processes forecast data files stored in specific directories, extracts metadata and forecast information, and loads the data into a PostgreSQL database. It is designed to handle two types of forecast models (day10 and day28) for different sites.

The source for the JSON files is the WeatherQuest data APIs. The APIs are queried daily using a scheduled Powerautomate workflow.

# Workflow
## Initialization:
- Reads configuration and establishes database connection. Connection information should be stored in a config.ini file (see below). Uses SQLAlchemy to connect to a PostgreSQL database, managing connections with a context manager.

## File Processing:
- Iterates through predefined folder structure.
- Opens and parses JSON files.
- Extracts metadata and data for processing.
- Checks if a record for the current metadata (last_updated and forecast_site) already exists in the database. If not, inserts new records.
- For each forecast:
  - Extracts the date and hour.
  - Inserts forecast information (e.g., forecast_day, forecast_hour) into the relevant table.
  - Loads associated forecast variable data (e.g., rainfall, temperature), and forecast summary data using load_variables_data.

## Database Insertion:
- Inserts metadata and forecast data.
- Handles the insertion of forecast variable data into two tables:
  - Model-specific forecast values (day10_forecast_model_values, day28_forecast_model_values).
  - Summary statistics for forecast variables (day10_forecast_variable_summaries, day28_forecast_variable_summaries).

# config.ini file
**THIS MUST BE ADDED TO GIT IGNORE**
```
[FORECAST_DATA]
username = your_username
password = your_password
host = your_host
port = your_port
database = your_database
```
