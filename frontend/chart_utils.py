import plotly.graph_objects as go
import pandas as pd


def get_feature_evolution(data):
    try:
        yesterday_date = data.index[-2]
        today_date = data.index[-1]

        yesterday_precipitation = data.loc[yesterday_date , "precipitation_hours"]
        yesterday_temperature = data.loc[yesterday_date , "temperature_2m_mean (°C)"]
        yesterday_surface_pressure = data.loc[yesterday_date , "surface_pressure_mean (hPa)"]
        yesterday_wind = data.loc[yesterday_date , "wind_speed_10m_max (m/s)"]

        today_precipitation = data.loc[today_date , "precipitation_hours"]
        today_temperature = data.loc[today_date , "temperature_2m_mean (°C)"]
        today_surface_pressure = data.loc[today_date , "surface_pressure_mean (hPa)"]
        today_wind = data.loc[today_date , "wind_speed_10m_max (m/s)"]

        precipitation_diff = (today_precipitation - yesterday_precipitation) / yesterday_precipitation * 100 if yesterday_precipitation != 0 else 0
        temperature_diff = (today_temperature - yesterday_temperature) / yesterday_temperature * 100 if yesterday_temperature != 0 else 0
        surface_pressure_diff = (today_surface_pressure - yesterday_surface_pressure) / yesterday_surface_pressure * 100 if yesterday_surface_pressure != 0 else 0
        wind_diff = (today_wind - yesterday_wind) / yesterday_wind * 100 if yesterday_wind != 0 else 0

        return (today_precipitation,today_temperature,today_surface_pressure,today_wind), (precipitation_diff, temperature_diff, surface_pressure_diff, wind_diff)

    except Exception as e:
        print(f"An error occurred during feature evolution computing: {e}")
        return None, None


def plot_and_display_data_predictions(
    data, rain_col="rain_sum (mm)", predicted_rain_col="predicted_rain (mm)"
):
    """
    Plots rain levels and includes predicted rain with a dashed green transition line.

    Parameters:
    - data (pd.DataFrame): DataFrame containing rain data.
    - discharge_col (str): Column name for the rain level.
    - predicted_discharge_col (str): Column name for predicted rain.
    """

    try:
        fig = go.Figure()
        
        # Plot known river discharge levels
        fig.add_trace(go.Scatter(
            x=data.index, 
            y=data[rain_col], 
            mode='lines',
            name='Rain Level',
            line=dict(color='blue')
        ))

        # Add predicted rain with a dashed green line from the last known value
        last_date = data.index[-1]  # Last date in the dataset
        if predicted_rain_col in data.columns and not data[predicted_rain_col].isna().all():
            next_date = last_date + pd.Timedelta(days=1)  # Next day's timestamp

            fig.add_trace(go.Scatter(
                x=data.index,
                y=data[predicted_rain_col],
                mode='lines+markers',
                name='Predicted Rain',
                line=dict(color='green', dash='dash'),
                marker=dict(size=8, color='green', symbol='circle')
            ))

        # Update layout
        fig.update_layout(
            title='Rain Levels with Forecast',
            xaxis_title='Date',
            yaxis_title='Rain Level (mm)',
            template='plotly_white'
        )

        return fig

    except Exception as e:
        print("Error plotting and displaying the data and predictions:", e)
        return None
