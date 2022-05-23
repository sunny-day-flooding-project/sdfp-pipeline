import pandas as pd
import numpy as np
import datetime
from atm_pressure import match_measurements_to_survey
from sqlalchemy import create_engine


# measurements = pd.read_csv("data/measurements.csv")
# measurements["date"] = pd.to_datetime(measurements["date"], utc = True)

# surveys = pd.read_csv("data/surveys.csv")
# surveys["date_surveyed"] = pd.to_datetime(surveys["date_surveyed"], utc = True)

# match_measurements_to_survey(measurements, surveys)

#######################
# Utility functions   #
#######################

def correct_drift(start, end):
    pass

########################
# Establish DB engine  #
########################

SQLALCHEMY_DATABASE_URL = "postgresql://" + os.environ.get('POSTGRESQL_USER') + ":" + os.environ.get(
    'POSTGRESQL_PASSWORD') + "@" + os.environ.get('POSTGRESQL_HOSTNAME') + "/" + os.environ.get('POSTGRESQL_DATABASE')

engine = create_engine(SQLALCHEMY_DATABASE_URL)

#####################
# Collect new data  #
#####################

try:
    new_data = pd.read_sql_query("SELECT * FROM sensor_data WHERE processed = 'FALSE' AND pressure > 800", engine).sort_values(['place','date']).drop_duplicates()
except:
    new_data = pd.DataFrame()
    warnings.warn("Connection to database failed to return data")
    
if new_data.shape[0] == 0:
    warnings.warn("- No new raw data!")
    pass

def get_smooth_min_wl(x):
    survey_dates = list(x["date_surveyed"].unique())
    
    smoothed_wl_df = pd.DataFrame()
    
    for selected_date in survey_dates:
        measurements = x.query("date_surveyed == @selected_date").copy()