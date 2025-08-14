import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st
from shared.model_utils import safe_predict_with_model , persistence_forecast
from shared.data_utils import fetch_and_prepare_data
 
from chart_utils import plot_and_display_data_predictions , get_feature_evolution
import datetime
from datetime import timedelta


st.set_page_config(page_title="Weather Forecast", layout="centered", page_icon="🌦")
st.title("🌦 Rain Forecasting App ")


@st.fragment(run_every="1d")
def get_input(start_date, end_date):
    data = fetch_and_prepare_data(start_date, end_date)
    return data

@st.fragment(run_every="1d1m")
def plot_predictions(data):
    fig = plot_and_display_data_predictions(data)
    st.plotly_chart(fig)
   

col1 , col2 = st.columns(2)

today = datetime.date.today()

#Fetch the data
two_days_ago = (today - timedelta(days=10))
start_date = col1.date_input("Start date", value = two_days_ago)
start_date = start_date if start_date <= today else two_days_ago
# start_date = start_date.strftime("%Y-%m-%d")

end_date = col2.date_input("End date", value = today)
end_date = end_date if end_date <= today else today
# end_date = end_date.strftime("%Y-%m-%d")

weather_df = get_input(start_date, end_date)

# Printing descriptive statistics about the river
col1, col2, col3 , col4 = st.columns(4)
today_features , features_evolution = get_feature_evolution(weather_df)
today_precipitation , today_temperature , today_surface_pressure , today_wind = today_features
precipitation_diff , temperature_diff , surface_pressure_diff , wind_diff = features_evolution


col1.metric("Surface Pressure", f"{today_surface_pressure:.2f} hPa", "{:.2f}%".format(surface_pressure_diff))
col2.metric("Precipitation", f"{today_precipitation:.2f} h", "{:.2f}%".format(precipitation_diff))
col3.metric("Temperature", f"{today_temperature:.2f} °C", "{:.2f}%".format(temperature_diff))
col4.metric("Wind", f"{today_wind:.2f} mph", "{:.2f}%".format(wind_diff))



# Getting the model and making the prediction
try:
    predicted_df = safe_predict_with_model(weather_df, horizon=7)
    
except Exception as e:
    st.warning(f"Failed to run model prediction: {e}")
    predicted_df = persistence_forecast(weather_df["rain_sum (mm)"], horizon=7)
    

full_index = weather_df.index.union(predicted_df.index)
weather_df = weather_df.reindex(full_index)
weather_df["predicted_rain (mm)"] = predicted_df



# Displaying the prediction and the river discharge
plot_predictions(weather_df)



with st.expander("Diagnostics & Model Status", expanded=False):
    st.write("Data range:", start_date, "→", end_date)
    st.write("Last 5 rows of data:")
    st.dataframe(weather_df[~weather_df["rain_sum (mm)"].isna()].tail(5))

# ----------------------------
# Footer
# ----------------------------
st.markdown('<div class="footer">Made with ❤️ — Weather Forecast · © 2025</div>', unsafe_allow_html=True)


