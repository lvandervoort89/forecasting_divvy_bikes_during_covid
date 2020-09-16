# Forecasting Divvy Bike Share Demand During Covid-19

## **Objective:**
Analyze before and during Covid-19 Divvy bike share demand. Forecast bike demand across all Divvy stations from September 1-December 31, 2020.

## **Approach:**
Divvy bike share data is cleaned and aggregated at the daily level. A time series model is built using Facebook Prophet. Custom seasonality was added to model the unique trends and changes brought on by Covid-19. A before/during Covid-19 weekly seasonality was added to capture the unique weekend popularity of demand. A Phase 1 (March 17-April 30), Phase 2 (May 1-June 2), and Phase 3 (June 3-25) yearly seasonality was added to capture the sharp decrease in demand brought on by the initial shutdown and then sharp increase in demand brought on by the various phases of Chicago's Covid-19 reopening. Demand was forecast for the remainder of the calendar year (September 1-December 31, 2020). Finally, a Tableau dashboard was built to show the unique trends and demand brought on by Covid-19.

## **Featured Techniques:**
- PostgreSQL
- SQLAlchemy
- Time Series forecasting
- Facebook Prophet
- Tableau

## **Data:**
Divvy bike share data was obtained from the [City of Chicago Data Portal](https://divvy-tripdata.s3.amazonaws.com/index.html) from January 1, 2017-August 31, 2020.

## **Results Summary:**
A Facebook Prophet time series model was optimized to have an average MAE of 2273.6 and average RMSE of 3002.7 across the forecast period. The optimized model with Covid-19 seasonality was better able to capture the sharp decrease in ride share demand at the beginning of shut down and the unique spike during the phases of reopening. It was also found that during Covid-19, weekends are uniquely popular (as compared to pre-covid) for bike rental. Additionally, while bike share demand dropped significantly at the start of shutdown, summer demand has been comparable, if not a bit higher, than previous years.

## **Tableau Visualizations:**
A Tableau dashboard was built to display the unique trends and shifts in demand for Divvy bikes during the Chicago phases of reopening.
