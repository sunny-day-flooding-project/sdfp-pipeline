import os
from unicodedata import numeric
from pytz import timezone
import requests
import datetime
import pandas as pd
from io import StringIO
from urllib.request import urlopen
import xmltodict
import numpy as np
import warnings
from sqlalchemy import create_engine

########################
# Utility functions    #
########################

def slicer(my_str,sub):
        index=my_str.find(sub)
        if index !=-1 :
            return my_str[index:] 
        else :
            raise Exception('Sub string not found!')
        
        
def postgres_upsert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)
    
    
def postgres_safe_insert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_nothing(
        constraint=f"{table.table.name}_pkey"
    )
    conn.execute(upsert_statement)
    

#############################
# Method-specific functions #
#############################

def get_noaa_atm(id, begin_date, end_date):
    """Retrieve atmospheric pressure data from the NOAA tides and currents API

    Args:
        id (str): Station id
        begin_date (str): Beginning date of requested time period. Format: %Y%m%d %H:%M
        end_date (str): End date of requested time period. Format: %Y%m%d %H:%M
        
    Returns:
        r_df (pd.DataFrame): DataFrame of atmospheric pressure from specified station and time range. Dates in UTC
    """    
    
    query = {'station' : str(id),
             'begin_date' : begin_date,
             'end_date' : end_date,
             'product' : 'air_pressure',
             'units' : 'metric',
             'time_zone' : 'gmt',
             'format' : 'json',
             'application' : 'Sunny_Day_Flooding_project, https://github.com/sunny-day-flooding-project'}
    
    r = requests.get('https://api.tidesandcurrents.noaa.gov/api/prod/datagetter/', params=query)
    
    j = r.json()
    
    r_df = pd.DataFrame.from_dict(j["data"])
    
    r_df["t"] = pd.to_datetime(r_df["t"], utc=True); r_df["id"] = str(id); r_df["notes"] = "coop"
    
    r_df = r_df.loc[:,["id","t","v","notes"]].rename(columns = {"id":"id","t":"date","v":"pressure_mb"})
    
    return r_df
    
def get_nws_atm(id, begin_date, end_date):
    """Retrieve atmospheric pressure data from the NWS API

    Args:
        id (str): Station id
        begin_date (str): Beginning date of requested time period. Format: %Y%m%d %H:%M
        end_date (str): End date of requested time period. Format: %Y%m%d %H:%M
        
    Returns:
        response (str): Still working on this!        
    """    
    
    new_begin_date = pd.to_datetime(begin_date, utc=True) - datetime.timedelta(seconds = 3600)
    new_end_date = pd.to_datetime(end_date, utc=True) + datetime.timedelta(seconds = 3600)

    query = {'start' : new_begin_date.isoformat(),
             'end' : new_end_date.isoformat()}
    
    r = requests.get("https://api.weather.gov/stations/" + str(id) + "/observations", params=query, headers = {'accept': 'application/geo+json'})
    
    j = r.json()
    
    # r_df = pd.DataFrame.from_dict(j["data"])
    
    # r_df["t"] = pd.to_datetime(r_df["t"], utc=True); r_df["id"] = id; r_df["notes"] = "coop"
    
    # r_df = r_df.loc[:,["id","t","v","notes"]].rename(columns = {"id":"id","t":"date","v":"pressure_mb"})
    
    pass

def get_isu_atm(id, begin_date, end_date):
    """Retrieve atmospheric pressure data from the ISU ASOS download service

    Args:
        id (str): Station id
        begin_date (str): Beginning date of requested time period. Format: %Y%m%d %H:%M
        end_date (str): End date of requested time period. Format: %Y%m%d %H:%M
    """   
    
    new_begin_date = pd.to_datetime(begin_date, utc=True) 
    new_end_date = pd.to_datetime(end_date, utc=True) 
    
    query = {'station' : str(id),
             'data' : 'all',
             'year1' : new_begin_date.year,
             'month1' : new_begin_date.month,
             'day1' : new_begin_date.day,
             'year2' : new_end_date.year,
             'month2' : new_end_date.month,
             'day2' : new_end_date.day + 1,
             'product' : 'air_pressure',
             'format' : 'comma',
             'latlon' : 'yes'
             }
    
    r = requests.get(url = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py', params=query, headers={'User-Agent' : 'Sunny_Day_Flooding_project, https://github.com/sunny-day-flooding-project'})
    
    s = slicer(str(r.content, 'utf-8'), "station")
    data = StringIO(s)
    
    r_df = pd.read_csv(filepath_or_buffer=data, lineterminator="\n", na_values=["","NA","M"])
    
    r_df["date"] = pd.to_datetime(r_df["valid"], utc=True); r_df["id"] = str(id); r_df["notes"] = "ISU"; r_df["pressure_mb"] = r_df["alti"] * 1000 * 0.0338639
    
    r_df = r_df.loc[:,["id","date","pressure_mb","notes"]].rename(columns = {"id":"id","t":"date","v":"pressure_mb"})
    
    return r_df

def get_fiman_atm(id, begin_date, end_date):
    """Retrieve atmospheric pressure data from the NOAA tides and currents API

    Args:
        id (str): Station id
        begin_date (str): Beginning date of requested time period. Format: %Y%m%d %H:%M
        end_date (str): End date of requested time period. Format: %Y%m%d %H:%M
        
    Returns:
        r_df (pd.DataFrame): DataFrame of atmospheric pressure from specified station and time range. Dates in UTC
    """    
    
    fiman_gauge_keys = pd.read_csv("data/fiman_gauge_key.csv").query("site_id == @id & Sensor == 'Barometric Pressure'")
    
    new_begin_date = pd.to_datetime(begin_date, utc=True) - datetime.timedelta(seconds = 3600)
    new_end_date = pd.to_datetime(end_date, utc=True) + datetime.timedelta(seconds = 3600)
    
    query = {'site_id' : fiman_gauge_keys.iloc[0]["site_id"],
             'data_start' : new_begin_date.strftime('%Y-%m-%d %H:%M:%S'),
             'end_date' : new_end_date.strftime('%Y-%m-%d %H:%M:%S'),
             'format_datetime' : '%Y-%m-%d %H:%M:%S',
             'tz' : 'utc',
             'show_raw' : True,
             'show_quality' : True,
             'sensor_id' : fiman_gauge_keys.iloc[0]["sensor_id"]}
    
    r = requests.get(os.environ.get("FIMAN_URL"), params=query)
    
    j = r.content
    
    doc = xmltodict.parse(j)
    
    unnested = doc["onerain"]["response"]["general"]["row"]
    
    r_df = pd.DataFrame.from_dict(unnested)

    r_df["date"] = pd.to_datetime(r_df["data_time"], utc=True); r_df["id"] = str(id); r_df["notes"] = "FIMAN"
    
    r_df = r_df.loc[:,["id","date","data_value","notes"]].rename(columns = {"data_value":"pressure_mb"})
    
    return r_df

#####################
# atm API functions #
#####################

def get_atm_pressure(atm_id, atm_src, begin_date, end_date):
    """Yo, yo, yo, it's a wrapper function!

    Args:
        atm_id (str): Value from `sensor_surveys` table that declares the ID of the station to use for atmospheric pressure data.
        atm_src (str): Value from `sensor_surveys` table that declares the source of the atmospheric pressure data.
        begin_date (str): The beginning date to retrieve data. Format: %Y%m%d %H:%M
        end_date (str): The end date to retrieve data. Format: %Y%m%d %H:%M

    Returns:
        pandas.DataFrame: Atmospheric pressure data for the specified time range and source
    """    
    match atm_src.upper():
        case "NOAA":
            return get_noaa_atm(id = atm_id, begin_date = begin_date, end_date = end_date)
        case "NWS":
            return get_nws_atm(id = atm_id, begin_date = begin_date, end_date = end_date)
        case "ISU":
            return get_isu_atm(id = atm_id, begin_date = begin_date, end_date = end_date)
        case "FIMAN":
            return get_fiman_atm(id = atm_id, begin_date = begin_date, end_date = end_date)
        case _:
            return "No valid `atm_src` provided! Make sure you are supplying a string"
        
        
def interpolate_atm_data(x, debug = True):
    place_names = list(x["place"].unique())
    
    interpolated_data = pd.DataFrame()
    
    for selected_place in place_names:
        selected_data = x.query("place == @selected_place").copy()
        selected_data["pressure_mb"] = np.nan
        
        dt_range = [selected_data["date"].min() - datetime.timedelta(seconds = 1800), selected_data["date"].max() + datetime.timedelta(seconds = 1800)]
        dt_duration = dt_range[1] - dt_range[0]
        dt_min = dt_range[0]
        dt_max = dt_range[1]
        
        if dt_duration >= datetime.timedelta(days=30):
            chunks = int(np.ceil(dt_duration / datetime.timedelta(days=30)))
            span = dt_duration / chunks
            
            atm_data = pd.DataFrame()
            for i in range(1, chunks + 1):
                range_min = dt_min + (span * (i-1))
                range_max = dt_min + (span * i)
                
                d = get_atm_pressure(atm_id = selected_data["atm_station_id"].unique()[0], 
                                            atm_src = selected_data["atm_data_src"].unique()[0], 
                                            begin_date = range_min.strftime("%Y%m%d %H:%M"),
                                            end_date = range_max.strftime("%Y%m%d %H:%M"))
                
                atm_data = pd.concat([atm_data, d]).drop_duplicates()
                
        if dt_duration < datetime.timedelta(days=30):      
                atm_data = get_atm_pressure(atm_id = selected_data["atm_station_id"].unique()[0], 
                                            atm_src = selected_data["atm_data_src"].unique()[0], 
                                            begin_date = dt_min.strftime("%Y%m%d %H:%M"),
                                            end_date = dt_max.strftime("%Y%m%d %H:%M")).drop_duplicates()     
            
        if(atm_data.empty):
            
            warnings.warn(message = f"No atm pressure data available for: {selected_place}")
            pass
                        
        combined_data = pd.concat([selected_data.query("date > @atm_data['date'].min() & date < @atm_data['date'].max()") , atm_data]).sort_values("date").set_index("date")
        combined_data["pressure_mb"] = combined_data["pressure_mb"].astype(float).interpolate(method='time')
                
        interpolated_data = pd.concat([interpolated_data, combined_data.loc[combined_data["place"].notna()].reset_index()[list(selected_data)]])

        if debug == True:
            print("####################################")
            print(f"- New raw data detected for: {selected_place}")
            print("- " , selected_data.shape[0] , " new rows")
            print("- Date duration is: ", dt_duration.days, " days")
            print("- " , selected_data.shape[0] - combined_data.loc[combined_data["place"].notna()].shape[0], "new observation(s) filtered out b/c not within atm pressure date range")
            print("####################################")
    
    return interpolated_data


def match_measurements_to_survey(measurements, surveys):
    sites = measurements["sensor_ID"].unique()
    survey_sites = surveys["sensor_ID"].unique()
    
    matching_sites = list(set(sites) & set(survey_sites))
    missing_sites = list(set(sites).difference(survey_sites))
    
    if len(missing_sites) > 0:
        warnings.warn(message = str("Missing survey data for: " + ''.join(missing_sites) + ". The site(s) will not be processed."))    
    
    matched_measurements = pd.DataFrame()
    
    for selected_site in matching_sites:
        selected_measurements = measurements.query("sensor_ID == @selected_site").copy()
        
        selected_survey = surveys.query("sensor_ID == @selected_site")
        
        if selected_survey.empty:
            warnings.warn("There are no survey data for: " + selected_site)
        
        survey_dates = list(selected_survey["date_surveyed"].unique())
        number_of_surveys = len(survey_dates)
        
        if measurements["date"].min() < min(survey_dates):
            warnings.warn("Warning: There are data that precede the survey dates for: " + selected_site)
            
        if number_of_surveys == 1:
            selected_measurements["date_surveyed"] = pd.to_datetime(np.where(selected_measurements["date"] >= survey_dates[0], survey_dates[0], np.nan))
            
        if number_of_surveys > 1:
            survey_dates.append(pd.to_datetime(datetime.datetime.utcnow(), utc=True))
            selected_measurements["date_surveyed"] = pd.to_datetime(pd.cut(selected_measurements["date"], bins = survey_dates, labels = survey_dates[:-1]), utc = True)
    
        merged_measurements_and_surveys = pd.merge(selected_measurements, surveys, how = "left", on = ["place","sensor_ID","date_surveyed"])
        
        matched_measurements = pd.concat([matched_measurements, merged_measurements_and_surveys]).drop_duplicates()
        matched_measurements["notes"] = matched_measurements["notes_x"]
        matched_measurements.drop(columns = ['notes_x','notes_y'],inplace=True)
        
    return matched_measurements


def format_interpolated_data(x):

    formatted_data = x.copy()
    formatted_data.rename(columns = {"pressure_mb":'atm_pressure', 'pressure':'sensor_pressure'}, inplace = True)
    formatted_data["sensor_water_depth"] = ((((formatted_data["sensor_pressure"] - formatted_data["atm_pressure"]) * 100) / (1020 * 9.81)) * 3.28084)
    formatted_data["qa_qc_flag"] = False; formatted_data["tag"] = "new_data"
    
    col_list = ["place","sensor_ID","date","atm_pressure","sensor_pressure","voltage","notes","sensor_water_depth","qa_qc_flag", "tag","atm_data_src","atm_station_id"]
    
    formatted_data = formatted_data.loc[:,col_list]
    
    formatted_data.set_index(['place', 'sensor_ID', 'date'], inplace=True)
    
    return formatted_data.drop_duplicates()


def main():
    
    # from env_vars import set_env_vars
    # set_env_vars()
    
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