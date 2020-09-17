'''
This script creates a dataframe used to create visualizations in Tableau.
'''

import datetime
import pandas as pd
import numpy as np

from geopy.geocoders import GoogleV3

def determine_covid(date):
    '''
    A helper fucntion that takes in a date and returns a value of 0 or 1 if the
    date is during Covid  (March 17-August 31, 2020).

    Parameters
    ----------
    date : The date of a Divvy bike ride.

    Returns
    -------
    A 0 if the date did not take place during Covid and a 1 if the ride took place during
    Covid.
    '''
    if date >= datetime.date(year=2020, month=3, day=17):
        return 1
    return 0


def phase_one(date):
    '''
    A helper fucntion that takes in a date and returns a value of 0 or 1 if the
    date is during Chicago's Phase 1 Covid response (March 17-April 30, 2020).

    Parameters
    ----------
    date : The date of a Divvy bike ride.

    Returns
    -------
    A 0 if the date did not take place during Phase 1 and a 1 if the ride took place during
    Phase 1.
    '''
    if (datetime.date(year=2020, month=3, day=17) <= date
            <= datetime.date(year=2020, month=4, day=30)):
        return 1
    return 0


def phase_two(date):
    '''
    A helper fucntion that takes in a date and returns a value of 0 or 1 if the
    date is during Chicago's Phase 2 Covid response (May 1-June 2, 2020).

    Parameters
    ----------
    date : The date of a Divvy bike ride.

    Returns
    -------
    A 0 if the date did not take place during Phase 2 and a 1 if the ride took place during
    Phase 2.
    '''
    if datetime.date(year=2020, month=5, day=1) <= date <= datetime.date(year=2020, month=6, day=2):
        return 1
    return 0


def phase_three(date):
    '''
    A helper fucntion that takes in a date and returns a value of 0 or 1 if the
    date is during Chicago's Phase 3 Covid response (June 3-June 25, 2020).

    Parameters
    ----------
    date : The date of a Divvy bike ride.

    Returns
    -------
    A 0 if the date did not take place during Phase 3 and a 1 if the ride took place during
    Phase 3.
    '''
    if (datetime.date(year=2020, month=6, day=3) <= date
            <= datetime.date(year=2020, month=6, day=25)):
        return 1
    return 0


def phase_four(date):
    '''
    A helper function that takes in a date and returns a value of 0 or 1 if the
    date is during Chicago's Phase 4 Covid response (June 26-August 31, 2020).

    Parameters
    ----------
    date : The date of a Divvy bike ride.

    Returns
    -------
    False if the date did not take place during Phase 4 and True if the ride took place during
    Phase 4.
    '''
    if date >= datetime.date(year=2020, month=6, day=26):
        return 1
    return 0


def add_covid_phase_dummys(all_data_2020):
    '''
    A function that takes adds Covid dummy variables to the 2020 bike share data.

    Parameters
    ----------
    all_data_2020 : The cleaned 2020 bike share data.

    Returns
    -------
    A dataframe with covid dummy variables.
    '''
    # Add in Covid dummy variable
    all_data_2020['covid'] = all_data_2020.apply(
        lambda x: determine_covid(x['start_day_of_year']), axis=1)

    # Add in phase dummy variables
    all_data_2020['phase_one'] = all_data_2020.apply(
        lambda x: phase_one(x['start_day_of_year']), axis=1)
    all_data_2020['phase_two'] = all_data_2020.apply(
        lambda x: phase_two(x['start_day_of_year']), axis=1)
    all_data_2020['phase_three'] = all_data_2020.apply(
        lambda x: phase_three(x['start_day_of_year']), axis=1)
    all_data_2020['phase_four'] = all_data_2020.apply(
        lambda x: phase_four(x['start_day_of_year']), axis=1)

    return all_data_2020


def phase_one_percent_change(all_data_2020, all_data_2019):
    '''
    A function that takes in 2 cleaned bike share dataframes and returns one
    dataframe with percent change added to it from phase 1.

    Parameters
    ----------
    all_data_2020 : The cleaned 2020 bike share data.
    all_data_2019 : The cleaned 2019 bike share data.

    Returns
    -------
    A dataframe with 2019 to 2020 phase 1 percent change column added.
    '''
    # 2020 Phase 1 Impact
    phase_one_data = all_data_2020[all_data_2020['phase_one'] == 1]
    phase_one_impact = phase_one_data.groupby(['from_station_id'], as_index=False).month.count()
    phase_one_impact = phase_one_impact.rename(columns={'month': 'number_rides_2020'})
    phase_one_impact = phase_one_impact.sort_values(by=['number_rides_2020'], ascending=False)

    # 2019 Phase 1 Impact
    phase_one_start_date = '03-17-2019'
    phase_one_end_date = '04-30-2019'
    phase_one_mask = ((all_data_2019['start_time'] >= phase_one_start_date)
                      & (all_data_2019['start_time'] <= phase_one_end_date))
    pre_covid_data_phase_one = all_data_2019.loc[phase_one_mask]
    pre_covid_impact_phase_one = pre_covid_data_phase_one.groupby(
        ['from_station_id'], as_index=False).month.count()
    pre_covid_impact_phase_one = pre_covid_impact_phase_one.rename(
        columns={'month': 'number_rides_2019'})
    pre_covid_impact_phase_one = pre_covid_impact_phase_one.sort_values(
        by=['number_rides_2019'], ascending=False)

    # Merge datasets together
    phase_one_2019_and_2020 = pd.merge(phase_one_impact, pre_covid_impact_phase_one,
                                       on='from_station_id')
    phase_one_2019_and_2020['phase_one_percent_change'] = (
        (phase_one_2019_and_2020['number_rides_2020'] -
         phase_one_2019_and_2020['number_rides_2019'])/
        phase_one_2019_and_2020['number_rides_2019'])
    phase_one_2019_and_2020 = phase_one_2019_and_2020.sort_values(
        by=['phase_one_percent_change'], ascending=False)

    # Rename columns to indicated number of rides from phase
    phase_one_2019_and_2020 = phase_one_2019_and_2020.rename(
        columns={'number_rides_2020': 'phase_1_number_rides_2020',
                 'number_rides_2019': 'phase_1_number_rides_2019'})

    return phase_one_2019_and_2020


def phase_two_percent_change(all_data_2020, all_data_2019):
    '''
    A function that takes in 2 cleaned bike share dataframes and returns one
    dataframe with percent change added to it from phase 2.

    Parameters
    ----------
    all_data_2020 : The cleaned 2020 bike share data.
    all_data_2019 : The cleaned 2019 bike share data.

    Returns
    -------
    A dataframe with 2019 to 2020 phase 2 percent change column added.
    '''
    # 2020 Phase 2 Impact
    phase_two_data = all_data_2020[all_data_2020['phase_two'] == 1]
    phase_two_impact = phase_two_data.groupby(['from_station_id'], as_index=False).month.count()
    phase_two_impact = phase_two_impact.rename(columns={'month': 'number_rides_2020'})
    phase_two_impact = phase_two_impact.sort_values(by=['number_rides_2020'], ascending=False)

    # 2019 Phase 2 Impact
    phase_two_start_date = '05-01-2019'
    phase_two_end_date = '06-02-2019'
    phase_two_mask = ((all_data_2019['start_time'] >= phase_two_start_date)
                      & (all_data_2019['start_time'] <= phase_two_end_date))
    pre_covid_data_phase_two = all_data_2019.loc[phase_two_mask]
    pre_covid_impact_phase_two = pre_covid_data_phase_two.groupby(
        ['from_station_id'], as_index=False).month.count()
    pre_covid_impact_phase_two = pre_covid_impact_phase_two.rename(
        columns={'month': 'number_rides_2019'})
    pre_covid_impact_phase_two = pre_covid_impact_phase_two.sort_values(
        by=['number_rides_2019'], ascending=False)

    # Merge datasets together
    phase_two_2019_and_2020 = pd.merge(phase_two_impact, pre_covid_impact_phase_two,
                                       on='from_station_id')
    phase_two_2019_and_2020['phase_two_percent_change'] = (
        (phase_two_2019_and_2020['number_rides_2020'] -
         phase_two_2019_and_2020['number_rides_2019'])/
        phase_two_2019_and_2020['number_rides_2019'])
    phase_two_2019_and_2020 = phase_two_2019_and_2020.sort_values(
        by=['phase_two_percent_change'], ascending=False)

    # Rename columns to indiciated number of rides from phase
    phase_two_2019_and_2020 = phase_two_2019_and_2020.rename(
        columns={'number_rides_2020': 'phase_2_number_rides_2020',
                 'number_rides_2019': 'phase_2_number_rides_2019'})

    return phase_two_2019_and_2020


def phase_three_percent_change(all_data_2020, all_data_2019):
    '''
    A function that takes in 2 cleaned bike share dataframes and returns one
    dataframe with percent change added to it from phase 3.

    Parameters
    ----------
    all_data_2020 : The cleaned 2020 bike share data.
    all_data_2019 : The cleaned 2019 bike share data.

    Returns
    -------
    A dataframe with 2019 to 2020 phase 3 percent change column added.
    '''
    # 2020 Phase 3 Impact
    phase_three_data = all_data_2020[all_data_2020['phase_three'] == 1]
    phase_three_impact = phase_three_data.groupby(['from_station_id'], as_index=False).month.count()
    phase_three_impact = phase_three_impact.rename(columns={'month': 'number_rides_2020'})
    phase_three_impact = phase_three_impact.sort_values(by=['number_rides_2020'], ascending=False)

    # 2019 Phase 3 Impact
    phase_three_start_date = '06-03-2019'
    phase_three_end_date = '06-25-2019'
    phase_three_mask = ((all_data_2019['start_time'] >= phase_three_start_date)
                        & (all_data_2019['start_time'] <= phase_three_end_date))
    pre_covid_data_phase_three = all_data_2019.loc[phase_three_mask]
    pre_covid_impact_phase_three = pre_covid_data_phase_three.groupby(
        ['from_station_id'], as_index=False).month.count()
    pre_covid_impact_phase_three = pre_covid_impact_phase_three.rename(
        columns={'month': 'number_rides_2019'})
    pre_covid_impact_phase_three = pre_covid_impact_phase_three.sort_values(
        by=['number_rides_2019'], ascending=False)

    # Merge datasets together
    phase_three_2019_and_2020 = pd.merge(phase_three_impact, pre_covid_impact_phase_three,
                                         on='from_station_id')
    phase_three_2019_and_2020['phase_three_percent_change'] = (
        (phase_three_2019_and_2020['number_rides_2020'] -
         phase_three_2019_and_2020['number_rides_2019'])/
        phase_three_2019_and_2020['number_rides_2019'])
    phase_three_2019_and_2020 = phase_three_2019_and_2020.sort_values(
        by=['phase_three_percent_change'], ascending=False)

    # Rename columns to indiciated number of rides from phase
    phase_three_2019_and_2020 = phase_three_2019_and_2020.rename(
        columns={'number_rides_2020': 'phase_3_number_rides_2020',
                 'number_rides_2019': 'phase_3_number_rides_2019'})

    return phase_three_2019_and_2020


def phase_four_percent_change(all_data_2020, all_data_2019):
    '''
    A function that takes in 2 cleaned bike share dataframes and returns one
    dataframe with percent change added to it from phase 4.

    Parameters
    ----------
    all_data_2020 : The cleaned 2020 bike share data.
    all_data_2019 : The cleaned 2019 bike share data.

    Returns
    -------
    A dataframe with 2019 to 2020 phase 4 percent change column added.
    '''
    # 2020 Phase 4 Impact
    phase_four_data = all_data_2020[all_data_2020['phase_four'] == 1]
    phase_four_impact = phase_four_data.groupby(['from_station_id'], as_index=False).month.count()
    phase_four_impact = phase_four_impact.rename(columns={'month': 'number_rides_2020'})
    phase_four_impact = phase_four_impact.sort_values(by=['number_rides_2020'], ascending=False)

    # 2019 Phase 4 Impact
    phase_four_start_date = '06-26-2019'
    phase_four_end_date = '08-31-2019'
    phase_four_mask = ((all_data_2019['start_time'] >= phase_four_start_date)
                       & (all_data_2019['start_time'] <= phase_four_end_date))
    pre_covid_data_phase_four = all_data_2019.loc[phase_four_mask]
    pre_covid_impact_phase_four = pre_covid_data_phase_four.groupby(
        ['from_station_id'], as_index=False).month.count()
    pre_covid_impact_phase_four = pre_covid_impact_phase_four.rename(
        columns={'month': 'number_rides_2019'})
    pre_covid_impact_phase_four = pre_covid_impact_phase_four.sort_values(
        by=['number_rides_2019'], ascending=False)

    # Merge datasets together
    phase_four_2019_and_2020 = pd.merge(phase_four_impact, pre_covid_impact_phase_four,
                                        on='from_station_id')
    phase_four_2019_and_2020['phase_four_percent_change'] = (
        (phase_four_2019_and_2020['number_rides_2020'] -
         phase_four_2019_and_2020['number_rides_2019'])/
        phase_four_2019_and_2020['number_rides_2019'])
    phase_four_2019_and_2020 = phase_four_2019_and_2020.sort_values(
        by=['phase_four_percent_change'], ascending=False)

    # Rename columns to indiciated number of rides from phase
    phase_four_2019_and_2020 = phase_four_2019_and_2020.rename(
        columns={'number_rides_2020': 'phase_4_number_rides_2020',
                 'number_rides_2019': 'phase_4_number_rides_2019'})

    return phase_four_2019_and_2020


def divvy_data_with_lat_lng(phase_one_2019_and_2020, phase_two_2019_and_2020,
                            phase_three_2019_and_2020, phase_four_2019_and_2020,
                            divvy_bike_stations):
    '''
    A function that takes in each phase percent change dataframe and the divvy bike
    stations with latitude and longitude information and returns one dataframe with
    everything merged together.

    Parameters
    ----------
    phase_one_2019_and_2020 : The phase one percent change dataframe.
    phase_two_2019_and_2020 : The phase two percent change dataframe.
    phase_three_2019_and_2020 : The phase three percent change dataframe.
    phase_four_2019_and_2020 : The phase four percent change dataframe.
    divvy_bike_stations : A dataframe with all 600+ Divvy stations and their
    latitude and longitude information.

    Returns
    -------
    A dataframe with all percent change data and station data merged together.
    '''
    phase_one_two = pd.merge(phase_one_2019_and_2020, phase_two_2019_and_2020,
                             left_on='from_station_id', right_on='from_station_id')
    phase_three_four = pd.merge(phase_three_2019_and_2020, phase_four_2019_and_2020,
                                left_on='from_station_id', right_on='from_station_id')
    all_data_percent_change_19_to_20 = pd.merge(phase_one_two, phase_three_four,
                                                left_on='from_station_id',
                                                right_on='from_station_id')

    # Merge all_data_percent_change_19_to_20 with station latitude and longtiude data
    all_data_with_lat_long = pd.merge(all_data_percent_change_19_to_20, divvy_bike_stations,
                                      left_on='from_station_id', right_on='ID')

    return all_data_with_lat_long


def get_zip_codes_from_google_api(all_data_with_lat_long):
    '''
    A function that takes in the Divvy station data and an API and gets zip codes
    for the Divvy stations using the Google Maps API.

    Parameters
    ----------
    all_data_with_lat_long : All divvy data with latitude and longitude information.
    your_api_key : Your API key to access Google Maps. Change this string to your
    unique API key.

    Returns
    -------
    A dataframe with zip codes for all Divvy stations.
    '''
    # Use Google Maps API to get zip code for each Divvy station
    geolocator = GoogleV3(api_key='your_api_key')

    zip_codes = []

    latitudes = all_data_with_lat_long['Latitude']
    longitudes = all_data_with_lat_long['Longitude']

    for lat, lng in zip(latitudes, longitudes):
        location = geolocator.reverse(str(lat) + ', ' + str(lng))
        cleaned_zip = int(location[0].split()[-2][:-1])
        zip_codes.append(cleaned_zip)

    # Convert zip code list into an array and add to dataframe
    zip_code_array = np.asarray(zip_codes)
    all_data_with_lat_long['zip_code'] = zip_code_array

    # Drop unnecessary columns
    zip_code_location_divvy_stations = all_data_with_lat_long.drop([
        'from_station_id',
        'phase_1_number_rides_2020', 'Longitude', 'Latitude',
        'phase_1_number_rides_2019', 'phase_one_percent_change',
        'phase_2_number_rides_2020', 'phase_2_number_rides_2019',
        'phase_two_percent_change', 'phase_3_number_rides_2020',
        'phase_3_number_rides_2019', 'phase_three_percent_change',
        'phase_4_number_rides_2020', 'phase_4_number_rides_2019',
        'phase_four_percent_change', 'ID', 'Station Name', 'Total Docks',
        'Docks in Service', 'Status'], axis=1)

    return zip_code_location_divvy_stations


def dataframe_for_tableau(zip_code_location_divvy_stations, all_data_with_lat_long,
                          chicago_zip_pop_data):
    '''
    A function that takes in the Divvy station data and an API and gets zip codes
    for the Divvy stations using the Google Maps API.

    Parameters
    ----------
    zip_code_location_divvy_stations : A dataframe with the zip code for every Divvy station
    all_data_with_lat_long : All divvy data with latitude and longitude information.
    chicago_zip_pop_data : A dataframe with total population by Chicago zip code.

    Returns
    -------
    A saved csv with all data needed to create tableau visualizations
    '''
    # Merge zip codes and all Divvy data
    all_divvy_with_zips = pd.merge(all_data_with_lat_long, zip_code_location_divvy_stations,
                                   left_on='Location', right_on='Location')

    # Convert zipcode column in chicago_zip_pop_data to integer type
    chicago_zip_pop_data['Geography'] = chicago_zip_pop_data.Geography.astype('int')

    # Merge chicago_zip_pop_data with all_divvy_with_zips
    all_data = pd.merge(all_divvy_with_zips, chicago_zip_pop_data,
                        left_on='zip_code', right_on='Geography', how='outer')

    # Remove unnecessary columns
    all_data = all_data.drop(['Population - Age 0-17', 'Population - Age 18-29', 'Year',
                              'Geography Type', 'Status', 'Docks in Service', 'Population - Latinx',
                              'Population - Age 30-39', 'Population - Age 40-49', 'Location',
                              'Total Docks', 'zip_code', 'ID', 'Population - Female',
                              'Population - Age 50-59', 'Population - Age 60-69', 'Record ID',
                              'Population - Age 70-79', 'Population - Age 80+',
                              'Population - Asian Non-Latinx', 'Population - Black Non-Latinx',
                              'Population - Male', 'Population - White Non-Latinx',
                              'Population - Other Race Non-Latinx'], axis=1)

    # Add percent change column multiplied by 100
    all_data['phase_one_percent_change_as_percent'] = all_data['phase_one_percent_change'] * 100
    all_data['phase_two_percent_change_as_percent'] = all_data['phase_two_percent_change'] * 100
    all_data['phase_three_percent_change_as_percent'] = all_data['phase_three_percent_change'] * 100
    all_data['phase_four_percent_change_as_percent'] = all_data['phase_four_percent_change'] * 100

    # Rename columns
    all_data.rename(columns={'from_station_id':'divvy_station_id', 'Geography': 'zip_code'})

    # Drop percent change as decimal columns
    all_data = all_data.drop(['phase_one_percent_change', 'phase_two_percent_change',
                              'phase_three_percent_change', 'phase_four_percent_change'],
                             axis=1)

    # Save file as csv to upload into Tableau
    all_data.to_csv(r'all_data.csv', index=False)

def main():
    '''
    Loads the cleaned ride datasets for 2020 and 2019 and outputs a dataframe to be
    used to create Tableau visualizations.
    '''

    all_data_2020 = pd.read_csv('all_data_2020.csv')
    all_data_2019 = pd.read_csv('all_data_2019.csv')
    divvy_bike_stations = pd.read_csv('../Data/Divvy_Bicycle_Stations.csv')
    chicago_zip_pop_data = pd.read_csv('../Data/Chicago_Population_Counts.csv')
    phase_one_2019_and_2020 = phase_one_percent_change(all_data_2020, all_data_2019)
    phase_two_2019_and_2020 = phase_two_percent_change(all_data_2020, all_data_2019)
    phase_three_2019_and_2020 = phase_three_percent_change(all_data_2020, all_data_2019)
    phase_four_2019_and_2020 = phase_four_percent_change(all_data_2020, all_data_2019)
    all_data_with_lat_long = divvy_data_with_lat_lng(phase_one_2019_and_2020,
                                                     phase_two_2019_and_2020,
                                                     phase_three_2019_and_2020,
                                                     phase_four_2019_and_2020,
                                                     divvy_bike_stations)
    zip_code_location_divvy_stations = get_zip_codes_from_google_api(all_data_with_lat_long)
    dataframe_for_tableau(zip_code_location_divvy_stations, all_data_with_lat_long,
                          chicago_zip_pop_data)

main()
