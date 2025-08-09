import os

import great_expectations as gx
from great_expectations import expectations as gxe
import pandas as pd
from datetime import datetime, timedelta 
from pathlib import Path

from custom_expectations import ExpectRainToBeZeroWhenPrecipitationHoursIsZero
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)

from great_expectations.expectations.expectation import register_expectation
register_expectation(ExpectRainToBeZeroWhenPrecipitationHoursIsZero)

parent_dir = Path(__file__).resolve().parents[2]

def setup_expectations(expectations_path ,**kwargs):
    """
    Setup the Great Expectations expectations for the weather data.
    Args:
        data_path (str): Path to the weather data CSV file.
        expectations_path (str): Path to store the Great Expectations configuration.
    """
    # Get the file path from the kwargs
    try:
        ti = kwargs['ti']
        filename = ti.xcom_pull(task_ids='DataFetching', key='weather_filename')
        data_path = parent_dir / 'data' / filename
    except KeyError:
        raise ValueError("No filename found in XCom. Ensure the data fetching task is executed before this task.")

    # Read the data
    df = pd.read_csv(data_path)
    context = gx.get_context(mode="file", project_root_dir=expectations_path)

    datasource = context.data_sources.add_pandas(name="weather_data_source")


    data_asset = datasource.add_dataframe_asset(name="weather_dataframe_asset")

    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # Define expectations
    # Every value in our dataset needs to be positive
    expectation_suite = gx.ExpectationSuite("weather_data_expectations")
    expectation_suite = context.suites.add(expectation_suite)

    # Table related expectation

    # Les colonnes doivent etre dans un set précis
    column_name_expectation = gxe.ExpectTableColumnsToMatchSet(
        column_set=["date",
                    "temperature_2m_max (°C)",
                    "temperature_2m_min (°C)",
                    "rain_sum (mm)",
                    "relative_humidity_2m_max (%)",
                    "relative_humidity_2m_min (%)",
                    "wind_speed_10m_max (m/s)",
                    "wind_speed_10m_min (m/s)",
                    "wind_speed_10m_mean (m/s)",
                    "relative_humidity_2m_mean (%)",
                    "cloudcover_mean (%)",
                    "surface_pressure_mean (hPa)",
                    "precipitation_hours"
        ],
        exact_match=False,
    )
    expectation_suite.add_expectation(
        column_name_expectation
    )

    # Validation des types
    date_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="date", type_="Timestamp")
    expectation_suite.add_expectation(
        date_type_expectation
    )

    rain_sum_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="rain_sum (mm)", type_="float64")
    expectation_suite.add_expectation(
        rain_sum_type_expectation
    )

    temp_max_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="temperature_2m_max (°C)", type_="float64")
    expectation_suite.add_expectation(
        temp_max_type_expectation
    )

    temp_min_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="temperature_2m_min (°C)", type_="float64")
    expectation_suite.add_expectation(
        temp_min_type_expectation
    )

    humidity_max_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="relative_humidity_2m_max (%)", type_="float64")
    expectation_suite.add_expectation(
        humidity_max_type_expectation
    )

    humidity_min_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="relative_humidity_2m_min (%)", type_="float64")
    expectation_suite.add_expectation(
        humidity_min_type_expectation
    )

    wind_speed_max_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="wind_speed_10m_max (m/s)", type_="float64")
    expectation_suite.add_expectation(
        wind_speed_max_type_expectation
    )

    wind_speed_min_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="wind_speed_10m_min (m/s)", type_="float64")
    expectation_suite.add_expectation(
        wind_speed_min_type_expectation
    )

    cloudcover_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="cloudcover_mean (%)", type_="float64")
    expectation_suite.add_expectation(
        cloudcover_type_expectation
    )

    surface_pressure_mean_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="surface_pressure_mean (hPa)", type_="float64")
    expectation_suite.add_expectation(
        surface_pressure_mean_type_expectation
    )

    precipitation_hours_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="precipitation_hours", type_="float64")
    expectation_suite.add_expectation(
        precipitation_hours_type_expectation
    )




    # Expectation 1 : la température maximale doit être supérieure à -5°C et inférieur à 50°C
    temp_max_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="temperature_2m_max (°C)",
        min_value=-5,
        max_value=50,
    )
    expectation_suite.add_expectation(
        temp_max_gt_0_exp,
    )

    # Expectation 2 : La température minimale doit être supérieure à -5°C et inférieur à 50°C
    temp_min_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="temperature_2m_min (°C)",
        min_value=-5,
        max_value=50,
    )
    expectation_suite.add_expectation(
        temp_min_gt_0_exp
    )

    # Expectation 3 : L'humidité maximale doit être comprise entre 0% et 100%
    humidity_max_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="relative_humidity_2m_max (%)",
        min_value=0,
        max_value=100,
    )
    expectation_suite.add_expectation(
        humidity_max_gt_0_exp
    )

    # Expectation 4 : L'humidité minimale doit être comprise entre 0% et 100%
    humidity_min_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="relative_humidity_2m_min (%)",
        min_value=0,
        max_value=100,
    )
    expectation_suite.add_expectation(
        humidity_min_gt_0_exp,
    )

    # Expectation 5 : La vitesse du vent maximale doit être supérieure à 0 et inférieure à 30 m/s
    ws_max_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="wind_speed_10m_max (m/s)",
        min_value=0,
        max_value=30,
    )
    expectation_suite.add_expectation(
        ws_max_gt_0_exp,
    )

    # Expectation 6 : La vitesse du vent minimale doit être supérieure à 0 et inférieure à 30 m/s
    ws_min_gt_30_exp = gxe.ExpectColumnValuesToBeBetween(
        column="wind_speed_10m_min (m/s)",
        min_value=0,
        max_value=30,
    )
    expectation_suite.add_expectation(
        ws_min_gt_30_exp,
    )

    # Expectation 7 : L'humidité moyenne doit être comprise entre 0% et 100%
    relative_humidity_2m_mean_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="relative_humidity_2m_mean (%)",
        min_value=0,
        max_value=100,
    )
    expectation_suite.add_expectation(
        relative_humidity_2m_mean_gt_0_exp,
    )

    # Expectation 8 : la couverture nuageuse moyenne doit être comprise entre 0% et 100%
    cloudcover_mean_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="cloudcover_mean (%)",
        min_value=0,
        max_value=100,
    )
    expectation_suite.add_expectation(
        cloudcover_mean_gt_0_exp,
    )

    # Expectation 9 : la pression atmosphérique moyenne doit être comprise entre 980 hPa et 1050 hPa
    surface_pressure_mean_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="surface_pressure_mean (hPa)",
        min_value=970,
        max_value=1050,
    )
    expectation_suite.add_expectation(
        surface_pressure_mean_gt_0_exp,
    )

    # Expectation 10 : Le nombre d'heures de précipitation doit être supérieur à 0 et inférieur à 24h
    precipitation_hours_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="precipitation_hours",
        min_value=0,
        max_value=24,
    )

    expectation_suite.add_expectation(
        precipitation_hours_gt_0_exp,
    )

    # Expectation 11 : Le somme des pluies doit être supérieur ou égal à 0 et inférieur à 150 mm
    rain_sum_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="rain_sum (mm)",
        min_value=0,
        max_value=150,
    )
    expectation_suite.add_expectation(
        rain_sum_gt_0_exp,
    )


    # Expectation 13: les dates doivent etre distinctes , uniques , non nulles 
    date_not_null_expectation = gxe.ExpectColumnValuesToNotBeNull(column="date")
    expectation_suite.add_expectation(
        date_not_null_expectation
    )

    
    date_uniqueness_expectation = gxe.ExpectColumnValuesToBeUnique(column="date")
    expectation_suite.add_expectation(
        date_uniqueness_expectation
    )

    # Expectation 14: Si precipitation_hours == 0, alors rain_sum == 0
    # rain_zero_when_precip_zero_expectation = ExpectRainToBeZeroWhenPrecipitationHoursIsZero(
    #     column_A="precipitation_hours",
    #     column_B="rain_sum"
    # )

    #rain_zero_when_precip_zero_config = rain_zero_when_precip_zero_expectation.to_expectation_configuration()
    #expectation_suite.add_expectation(
    #    rain_zero_when_precip_zero_config
    #)


