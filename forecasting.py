'''
This script performs time series forecasting on Divvy bike share data using
Facebook Prophet.
'''

import datetime as dt
import pickle
import pandas as pd
from fbprophet import Prophet
from fbprophet.diagnostics import performance_metrics
from fbprophet.diagnostics import cross_validation

def is_covid(ds):
    '''
    A helper fucntion that takes in a date and returns a value of 0 or 1 if the
    date is before or during Covid-19 (indicated by the initial date of Chicago's shut
    down).

    Parameters
    ----------
    date : The date of a Divvy bike ride.

    Returns
    -------
    A 0 if the date took place before Covid-19 and a 1 if the ride took place during
    Covid-19.
    '''
    date = pd.to_dt(ds)
    return date >= dt.date(year=2020, month=3, day=17)


def is_phase_one(ds):
    '''
    A helper fucntion that takes in a date and returns a value of 0 or 1 if the
    date is during Chicago's Phase 1 Covid response (March 17-April 30, 2020).

    Parameters
    ----------
    date : The date of a Divvy bike ride.

    Returns
    -------
    False if the date did not take place during Phase 1 and True if the ride took place during
    Phase 1.
    '''
    date = pd.to_dt(ds)
    return dt.date(year=2020, month=3, day=17) <= date <= dt.date(year=2020, month=4, day=30)


def is_phase_two(ds):
    '''
    A helper fucntion that takes in a date and returns a value of 0 or 1 if the
    date is during Chicago's Phase 2 Covid response (May 1-June 2, 2020).

    Parameters
    ----------
    date : The date of a Divvy bike ride.

    Returns
    -------
    False if the date did not take place during Phase 2 and True if the ride took place during
    Phase 2.
    '''
    date = pd.to_dt(ds)
    return dt.date(year=2020, month=5, day=1) <= date <= dt.date(year=2020, month=6, day=2)


def is_phase_three(ds):
    '''
    A helper fucntion that takes in a date and returns a value of 0 or 1 if the
    date is during Chicago's Phase 3 Covid response (June 3-June 25, 2020).

    Parameters
    ----------
    date : The date of a Divvy bike ride.

    Returns
    -------
    False if the date did not take place during Phase 3 and True if the ride took place during
    Phase 3.
    '''
    date = pd.to_dt(ds)
    return dt.date(year=2020, month=6, day=3) <= date <= dt.date(year=2020, month=6, day=25)


def final_model(dataframe):
    '''
    A function that pickles a Facebook Prophet time series model using Chicago's
    phased Covid response and reopening. Dates are Chicago specific.

    Parameters
    ----------
    df : A dataframe containing Divvy ride share data at the daily level.

    Returns
    -------
    A pickled Prophet model, a cross validated results dataframe containing the
    forecast for the remaining 122 days of the year, and a perforamnce metrics
    dataframe.
    '''
    # Rename columns to match requirement for Prophet
    dataframe = dataframe.rename(columns={'number_daily_rides': 'y', 'start_day_of_year': 'ds'})

    # Drop all other columns
    dataframe = dataframe.drop(['from_station_id'], axis=1)

    # Add columns for Covid seasonality
    dataframe['covid'] = dataframe['ds'].apply(is_covid)
    dataframe['precovid'] = ~dataframe['ds'].apply(is_covid)

    # Add columns for Phase 1 seasonality
    dataframe['phase_one'] = dataframe['ds'].apply(is_phase_one)
    dataframe['not_phase_one'] = ~dataframe['ds'].apply(is_phase_one)

    # Add columns for Phase 2 seasonality
    dataframe['phase_two'] = dataframe['ds'].apply(is_phase_two)
    dataframe['not_phase_two'] = ~dataframe['ds'].apply(is_phase_two)

    # Add columns for Phase 3 seasonality
    dataframe['phase_three'] = dataframe['ds'].apply(is_phase_three)
    dataframe['not_phase_three'] = ~dataframe['ds'].apply(is_phase_three)

    # Run model
    prophet = Prophet(daily_seasonality=False, weekly_seasonality=False, yearly_seasonality=False,
                      seasonality_prior_scale=20, changepoint_prior_scale=0.2)

    prophet.add_seasonality(name='covid', period=7, fourier_order=10, condition_name='covid')
    prophet.add_seasonality(name='precovid', period=7, fourier_order=3, condition_name='precovid')
    prophet.add_seasonality(
        name='phase_one', period=365.25, fourier_order=20, condition_name='phase_one')
    prophet.add_seasonality(
        name='not_phase_one', period=365.25, fourier_order=3, condition_name='not_phase_one')
    prophet.add_seasonality(
        name='phase_two', period=365.25, fourier_order=3, condition_name='phase_two')
    prophet.add_seasonality(
        name='not_phase_two', period=365.25, fourier_order=3, condition_name='not_phase_two')
    prophet.add_seasonality(
        name='phase_three', period=365.25, fourier_order=10, condition_name='phase_three')
    prophet.add_seasonality(
        name='not_phase_three', period=365.25, fourier_order=3, condition_name='not_phase_three')
    prophet.add_country_holidays(country_name='US')

    prophet.fit(dataframe)

    future = prophet.make_future_dataframe(periods=122)
    future['covid'] = future['ds'].apply(is_covid)
    future['precovid'] = ~future['ds'].apply(is_covid)
    future['phase_one'] = future['ds'].apply(is_phase_one)
    future['not_phase_one'] = ~future['ds'].apply(is_phase_one)
    future['phase_two'] = future['ds'].apply(is_phase_two)
    future['not_phase_two'] = ~future['ds'].apply(is_phase_two)
    future['phase_three'] = future['ds'].apply(is_phase_three)
    future['not_phase_three'] = ~future['ds'].apply(is_phase_three)

    forecast = prophet.predict(future)

    cv_results = cross_validation(
        prophet, initial='730 days', period='180 days', horizon='122 days')
    performance_results = performance_metrics(cv_results, metrics=['mae', 'rmse'])

    # Pickle the model
    with open(prophet.pkl, "wb") as f:
        pickle.dump(prophet, f)

    # Pickle the forecast dataframe
    forecast.to_pickle("forecast.pkl")

    # Pickle the performance results
    performance_results.to_pickle("performance_results.pkl")


def main():
    '''
    Loads the daily ride dataset and outputs a FacebookProphet model
    '''

    daily_df_2017_to_2020 = pd.read_csv('daily_df_2017_to_2020.csv')
    final_model(daily_df_2017_to_2020)

main()
