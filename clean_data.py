'''
This script creates a connection to a local PostgreSQL database and pulls data
needed to forecast Divvy demand. Data is then cleaned and the resulting
tables are saved as a csv file.
'''
from sqlalchemy import create_engine
import pandas as pd

def sql_to_dataframe():
    '''
    A function that connects to a local PostgreSQL database and queries Divvy
    bikeshare into yearly dataframes.

    Returns
    -------
    A dataframe for each year of Divvy data (4 in total).
    '''
    # Use SQLAlchemy to connect to PostgreSQL database
    cnx = create_engine('postgresql://lisavandervoort@localhost:5432/divvy')

    # Create dataframes containing each year's individual ride data
    all_data_2020 = pd.read_sql_query('''SELECT * FROM jan_feb_march2020
                                UNION SELECT * FROM april2020
                                UNION SELECT * FROM may2020
                                UNION SELECT * FROM june2020
                                UNION SELECT * FROM july2020
                                UNION SELECT * FROM august2020
                            ''', cnx)

    all_data_2019 = pd.read_sql_query('''SELECT * FROM jan_feb_march2019
                                UNION SELECT * FROM april_may_june2019
                                UNION SELECT * FROM july_aug_sept2019
                                UNION SELECT * FROM oct_nov_dec2019
                            ''', cnx)

    all_data_2018 = pd.read_sql_query('''SELECT * FROM jan_feb_march2018
                                UNION SELECT * FROM april_may_june2018
                                UNION SELECT * FROM july_aug_sept2018
                                UNION SELECT * FROM oct_nov_dec2018
                            ''', cnx)

    all_data_2017 = pd.read_sql_query('''SELECT * FROM jan_feb_march2017
                                UNION SELECT * FROM april_may_june2017
                                UNION SELECT * FROM july_aug_sept2017
                                UNION SELECT * FROM oct_nov_dec2017
                            ''', cnx)


    return all_data_2020, all_data_2019, all_data_2018, all_data_2017

def clean_and_engineer_dataframes(all_data_2020, all_data_2019, all_data_2018, all_data_2017):
    '''
    A function that cleans the yearly Divvy data and saves the dataframes to csv files.

    Parameters
    ----------
    all_data_2020, all_data_2019, all_data_2018, all_data_2017 : Divvy bike share data

    Returns
    -------
    This saves each yearly dataframe cleaned as a csv file and a daily ride dataframe as a csv.
    '''
    ## Clean 2020 dataframe
    # Drop nulls
    all_data_2020.dropna(inplace=True)

    # Drop rows with electric bikes
    all_data_2020.drop(
        all_data_2020[all_data_2020['rideable_type'] == 'electric_bike'].index, inplace=True)

    # Rename columns to match prior years
    all_data_2020 = all_data_2020.rename(
        columns={'started_at':'start_time', 'start_station_id': 'from_station_id'})

    # Create new column with day of year
    all_data_2020['start_day_of_year'] = all_data_2020.start_time.dt.date

    # Create new column with month
    all_data_2020['month'] = all_data_2020.start_time.dt.month

    # Drop unnecessary columns
    all_data_2020 = all_data_2020.drop(
        ['ride_id', 'rideable_type', 'ended_at', 'start_station_name',
         'end_station_id', 'member_casual', 'end_station_name', 'start_lat', 'start_lng',
         'end_lat', 'end_lng'], axis=1)


    ## Clean 2019 dataframe
    # Create new column with day of year
    all_data_2019['start_day_of_year'] = all_data_2019.start_time.dt.date # pylint: disable=no-member

    # Create new column with month
    all_data_2019['month'] = all_data_2019.start_time.dt.month # pylint: disable=no-member

    # Drop unnecessary columns
    all_data_2019 = all_data_2019.drop(
        ['trip_id', 'end_time', 'bikeid', 'tripduration', 'from_station_name',
         'to_station_id', 'gender', 'to_station_name', 'birthyear', 'usertype'], axis=1)


    ## Clean 2018 dataframe
    # Create new column with day of year
    all_data_2018['start_day_of_year'] = all_data_2018.start_time.dt.date # pylint: disable=no-member

    # Create new column with month
    all_data_2018['month'] = all_data_2018.start_time.dt.month # pylint: disable=no-member

    # Drop unnecessary columns
    all_data_2018 = all_data_2018.drop(
        ['trip_id', 'end_time', 'bikeid', 'tripduration', 'from_station_name',
         'to_station_id', 'gender', 'to_station_name', 'birthyear', 'usertype'], axis=1)


    ## Clean 2017 dataframe
    # Create new column with day of year
    all_data_2017['start_day_of_year'] = all_data_2017.start_time.dt.date # pylint: disable=no-member

    # Create new column with month
    all_data_2017['month'] = all_data_2017.start_time.dt.month # pylint: disable=no-member

    # Drop unnecessary columns
    all_data_2017 = all_data_2017.drop(
        ['trip_id', 'end_time', 'bikeid', 'tripduration', 'from_station_name',
         'to_station_id', 'gender', 'to_station_name', 'birthyear', 'usertype'], axis=1)

    # Concatenate 2017-2020 data together
    all_data_2017_to_2020 = pd.concat(
        [all_data_2020, all_data_2019, all_data_2018, all_data_2017], ignore_index=True)

    # Group data by day
    daily_df_2017_to_2020 = all_data_2017_to_2020.groupby(
        ['from_station_id', 'start_day_of_year'], as_index=False).month.count()
    daily_df_2017_to_2020 = daily_df_2017_to_2020.rename(
        columns={'month': 'number_daily_rides'})

    # Save dataframes to csv files
    all_data_2020.to_csv(r'all_data_2020.csv', index=False)
    all_data_2019.to_csv(r'all_data_2019.csv', index=False)
    all_data_2018.to_csv(r'all_data_2018.csv', index=False)
    all_data_2017.to_csv(r'all_data_2017.csv', index=False)

    # Save daily dataframe
    daily_df_2017_to_2020.to_csv(r'daily_df_2017_to_2020.csv', index=False)

def main():
    '''
    Calls internal functions to the script to pull data from PostgreSQL, clean data,
    create additional dataframes, and engineer features. All dataframes are saved as csv
    files.
    '''

    # Call internal functions to this script
    all_data_2020, all_data_2019, all_data_2018, all_data_2017 = sql_to_dataframe()
    clean_and_engineer_dataframes(all_data_2020, all_data_2019, all_data_2018, all_data_2017)

main()
