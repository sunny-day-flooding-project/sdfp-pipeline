from atm_pressure import *
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def main():
    
    from env_vars import set_env_vars
    set_env_vars()
    print("set env vars!")
    
    ########################
    # Establish DB engine  #
    ########################

    SQLALCHEMY_DATABASE_URL = "postgresql://" + os.environ.get('POSTGRESQL_USER') + ":" + os.environ.get(
        'POSTGRESQL_PASSWORD') + "@" + os.environ.get('POSTGRESQL_HOSTNAME') + "/" + os.environ.get('POSTGRESQL_DATABASE')

    engine = create_engine(SQLALCHEMY_DATABASE_URL)

    print(engine)
    
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
        return
        
    sensors_w_new_data = list(new_data["sensor_ID"].unique())
    
    try:
        surveys = pd.read_sql_table("sensor_surveys", engine).sort_values(['place','date_surveyed']).drop_duplicates()
    except:
        surveys = pd.DataFrame()
        warnings.warn("Connection to database failed to return data")
        
    if surveys.shape[0] == 0:
        warnings.warn("- No survey data!")
        return
        
    prepared_data = match_measurements_to_survey(measurements = new_data, surveys = surveys)
    
    try: 
        interpolated_data = interpolate_atm_data(prepared_data)
    except: 
        interpolated_data = pd.DataFrame()
    
    if interpolated_data.shape[0] == 0:
        warnings.warn("No data to write to database!")

        return "No data to write to database!"
    
    formatted_data = format_interpolated_data(interpolated_data)
    
    # Upsert the new data to the database table
    try:
        formatted_data.to_sql("sensor_water_depth", engine, if_exists = "append", method=postgres_upsert)
        print("Processed data to produce water depth!")
    except:
        warnings.warn("Error adding processed data to `sensor_water_depth`")
    
    updated_raw_data = new_data.merge(formatted_data.reset_index().loc[:,["place","sensor_ID","date","sensor_water_depth"]], on=["place","sensor_ID","date"], how = "left")
    updated_raw_data = updated_raw_data[updated_raw_data["sensor_water_depth"].notna()].drop(columns="sensor_water_depth")
    updated_raw_data["processed"] = True
    
    updated_raw_data.set_index(['place', 'sensor_ID', 'date'], inplace=True)
    
    # Update raw data to indicate it has been processed
    try:
        updated_raw_data.to_sql("sensor_data", engine, if_exists = "append", method=postgres_upsert)
        print("Updated raw data to indicate that it was processed!")
    except:
        warnings.warn("Error updating raw data with `processed` tag")
    
    engine.dispose()

if __name__ == "__main__":
    main()