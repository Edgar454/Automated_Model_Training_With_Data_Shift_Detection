# app.py
import os
from pathlib import Path
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

from variables import GLASS_CSS

# Darts imports (optional; app works without them if not installed / model missing)
try:
    from darts import TimeSeries
    from darts.models import CatBoostModel
    DARTS_AVAILABLE = True
except Exception:
    DARTS_AVAILABLE = False

# ----------------------------
# Configuration
# ----------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / "data" / "weather_data.csv"
MODEL_PATH = BASE_DIR / "models" / "rain_forecasting_model" / "catboost_model.pkl"
FORECAST_HORIZON = 7  

st.set_page_config(page_title="Weather Forecast", layout="centered", page_icon="🌦")

# ----------------------------
# Styling (glass cards)
# ----------------------------
st.markdown(GLASS_CSS, unsafe_allow_html=True)

# ----------------------------
# Utility helpers
# ----------------------------
@st.cache_data(ttl=600)
def load_data(path: Path):
    if not path.exists():
        return None
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    df = df.sort_index().dropna()
    return df

def build_fourier(index, freq=365.25, order=2):
    t = np.arange(len(index), dtype=np.float32)
    k = 2 * np.pi * (1 / freq) * t
    out = {}
    for i in range(1, order+1):
        out[f"sin_{freq}_{i}"] = np.sin(i*k)
        out[f"cos_{freq}_{i}"] = np.cos(i*k)
    return pd.DataFrame(out, index=index)

def persistence_forecast(last_values: pd.Series, horizon: int):
    """Simple persistence forecast: repeat last known value (or mean)"""
    last = last_values.iloc[-1]
    return pd.Series([last]*horizon, index=[last_values.index[-1] + timedelta(days=i) for i in range(1,horizon+1)])

def safe_predict_with_model(model_obj, rain_ts, past_covariates_ts, horizon=7):
    """Try to predict with darts model; handle exceptions."""
    try:
        pred = model_obj.predict(horizon, past_covariates=past_covariates_ts)
        # convert to pandas series
        if hasattr(pred, "pd_dataframe"):
            pred_df = pred.pd_dataframe()
            # if univariate
            if pred_df.shape[1] == 1:
                series = pred_df.iloc[:,0]
            else:
                series = pred_df.iloc[:,0]
            return series
        else:
            return None
    except Exception as e:
        st.warning(f"Model prediction failed: {e}")
        return None

# ----------------------------
# Load data & basic UI inputs
# ----------------------------
df = load_data(DATA_PATH)

st.markdown('<div class="header">', unsafe_allow_html=True)
st.markdown('<div class="title">🌦 Weather Forecast</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Powered by your local AI Rain Predictor</div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="glass">', unsafe_allow_html=True)
col1, col2, col3 = st.columns([2,3,1])
with col1:
    selected_date = st.date_input("Date", value=datetime.utcnow().date())
with col2:
    # location list could be dynamic; here we keep one default option
    location = st.selectbox("Location", options=["Brazzaville (default)"])
with col3:
    get_btn = st.button("Get Forecast")
st.markdown('</div>', unsafe_allow_html=True)

# ----------------------------
# Data checks and preparation
# ----------------------------
if df is None:
    st.error("No data found at 'data/weather_data.csv'. Place your CSV there or modify DATA_PATH.")
    st.stop()

# Create 2 Fourier features if not present
fourier_df = build_fourier(df.index, freq=365.25, order=2)
for c in fourier_df.columns:
    if c not in df.columns:
        df[c] = fourier_df[c]

# Prepare latest inputs
target_col = "rain_sum (mm)"
past_covariate_cols = [
    "temperature_2m_max (°C)",
    "wind_speed_10m_min (m/s)",
    "surface_pressure_mean (hPa)",
    "precipitation_hours",
    "day_of_the_year",
    "cos_365.25_1",
    "cos_365.25_2",
]
# ensure columns exist
past_covariate_cols = [c for c in past_covariate_cols if c in df.columns]

# Use last available dates for "current" metrics
latest_row = df.iloc[-1]
current_temp = latest_row.get("temperature_2m_max (°C)", np.nan)
current_wind = latest_row.get("wind_speed_10m_min (m/s)", np.nan)
current_pressure = latest_row.get("surface_pressure_mean (hPa)", np.nan)
current_precip = latest_row.get("precipitation_hours", np.nan)
current_rain = latest_row.get(target_col, np.nan)

# ----------------------------
# Try to load the Darts model
# ----------------------------
model_loaded = None
if DARTS_AVAILABLE and Path(MODEL_PATH).exists():
    try:
        model_loaded = CatBoostModel.load(str(MODEL_PATH))
    except Exception:
        # Darts wrapper might have different saving; try to load bare if possible
        try:
            # sometimes the saved object might be the underlying catboost model serialized differently
            model_loaded = None
        except Exception:
            model_loaded = None

# ----------------------------
# Forecasting logic
# ----------------------------
forecast_series = None
if get_btn:
    st.info("Computing forecast...")
    # Build TimeSeries for darts if available
    if DARTS_AVAILABLE and model_loaded is not None:
        try:
            rain_ts = TimeSeries.from_dataframe(df, value_cols=[target_col])
            if len(past_covariate_cols) > 0:
                past_cov_ts = TimeSeries.from_dataframe(df, value_cols=past_covariate_cols)
            else:
                past_cov_ts = None

            pred = safe_predict_with_model(model_loaded, rain_ts, past_cov_ts, horizon=FORECAST_HORIZON)
            if pred is not None:
                # ensure index are datelike
                forecast_series = pd.Series(pred.values, index=pd.to_datetime(pred.index))
        except Exception as e:
            st.warning(f"Failed running darts model prediction: {e}")
            forecast_series = None

    # fallback: persistence baseline using last rainfall value
    if forecast_series is None:
        st.info("Using persistence baseline forecast (model not available).")
        forecast_series = persistence_forecast(df[target_col], FORECAST_HORIZON)
else:
    # do not compute until button is pressed; show a small preview forecast using persistence
    forecast_series = persistence_forecast(df[target_col], FORECAST_HORIZON)

# ----------------------------
# Display main card and KPIs
# ----------------------------
st.markdown('<div class="glass" style="margin-top:14px">', unsafe_allow_html=True)
col_main, col_side = st.columns([3,1])
with col_main:
    # icon selection roughly by last rain
    if current_rain == 0 or np.isnan(current_rain):
        icon = "🌤"
        desc = "No rain expected"
    elif current_rain < 5:
        icon = "🌤"
        desc = "Light precipitation possible"
    elif current_rain < 20:
        icon = "🌧"
        desc = "Moderate rain likely"
    else:
        icon = "⛈"
        desc = "Heavy rainfall expected"

    st.markdown(f'<div class="big-temp"><div style="font-size:48px">{icon}</div>'
                f'<div><div class="temp">{round(current_temp,1) if not np.isnan(current_temp) else "—"}°C</div>'
                f'<div style="color:#27557a; opacity:0.9">{desc}</div></div></div>', unsafe_allow_html=True)

with col_side:
    st.markdown('<div class="small-grid">', unsafe_allow_html=True)
    st.markdown(f'<div class="kpi"><div style="font-size:20px; font-weight:700">{round(current_temp,1) if not np.isnan(current_temp) else "—"}°C</div><div style="opacity:.8">Current</div></div>', unsafe_allow_html=True)
    st.markdown(f'<div class="kpi"><div style="font-size:18px; font-weight:700">{round(latest_row.get("temperature_2m_max (°C)",np.nan),1) if not np.isnan(latest_row.get("temperature_2m_max (°C)")) else "—"}°C</div><div style="opacity:.8">Max</div></div>', unsafe_allow_html=True)
    st.markdown(f'<div class="kpi"><div style="font-size:18px; font-weight:700">{round(latest_row.get("wind_speed_10m_min (m/s)",np.nan),1) if not np.isnan(latest_row.get("wind_speed_10m_min (m/s)")) else "—"} m/s</div><div style="opacity:.8">Wind</div></div>', unsafe_allow_html=True)
    st.markdown(f'<div class="kpi"><div style="font-size:18px; font-weight:700">{round(latest_row.get("surface_pressure_mean (hPa)",np.nan),1) if not np.isnan(latest_row.get("surface_pressure_mean (hPa)")) else "—"}</div><div style="opacity:.8">Pressure</div></div>', unsafe_allow_html=True)
    st.markdown(f'<div class="kpi"><div style="font-size:18px; font-weight:700">{round(latest_row.get("rain_sum (mm)",np.nan),2) if not np.isnan(latest_row.get("rain_sum (mm)")) else "—"} mm</div><div style="opacity:.8">Rain (last)</div></div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# ----------------------------
# Forecast chart (temperature / rain toggle)
# ----------------------------
st.markdown('<div class="glass" style="margin-top:14px">', unsafe_allow_html=True)
st.write("### Forecast")

# Build DataFrame for the chart (forecast_series is a pandas Series)
if isinstance(forecast_series, pd.Series):
    df_fore = forecast_series.rename("predicted_rain").to_frame()
    df_fore["date"] = pd.to_datetime(df_fore.index)
    df_fore = df_fore.reset_index(drop=True)
else:
    df_fore = pd.DataFrame({
        "date": [datetime.utcnow().date() + timedelta(days=i+1) for i in range(FORECAST_HORIZON)],
        "predicted_rain": [0.0]*FORECAST_HORIZON
    })

chart_type = st.radio("Metric to show", ["Rain", "Temperature"], horizontal=True)
if chart_type == "Rain":
    fig = px.line(df_fore, x="date", y="predicted_rain", markers=True, title="Rain forecast (mm)")
    fig.update_traces(line=dict(color="#2b8cff"), marker=dict(size=8))
    fig.update_layout(margin=dict(l=10,r=10,t=40,b=10), height=320)
    st.plotly_chart(fig, use_container_width=True)
else:
    # temperature: show past few temps + a flat persistence forecast for demo
    temp_idx = df.index[-(30):]
    temp_df = pd.DataFrame({
        "date": temp_idx,
        "temperature": df.loc[temp_idx, "temperature_2m_max (°C)"].values
    }).reset_index(drop=True)
    # add forecasted flat line
    fut_dates = [temp_df["date"].iloc[-1] + timedelta(days=i+1) for i in range(FORECAST_HORIZON)]
    fut_temps = [temp_df["temperature"].iloc[-1]]*FORECAST_HORIZON
    temp_plot_df = pd.concat([temp_df, pd.DataFrame({"date": fut_dates, "temperature": fut_temps})], ignore_index=True)
    fig2 = px.line(temp_plot_df, x="date", y="temperature", title="Temperature (°C)", markers=True)
    fig2.update_layout(margin=dict(l=10,r=10,t=40,b=10), height=320)
    st.plotly_chart(fig2, use_container_width=True)

st.markdown('</div>', unsafe_allow_html=True)

# ----------------------------
# Residuals / diagnostics quick view
# ----------------------------
with st.expander("Diagnostics & Model Status", expanded=False):
    st.write("Model loaded:" , bool(model_loaded))
    if DARTS_AVAILABLE:
        st.write("Darts available:", True)
    else:
        st.write("Darts available:", False)

    st.write("Data range:", df.index.min().date(), "→", df.index.max().date())
    st.write("Last 5 rows of data:")
    st.dataframe(df.tail(5))

# ----------------------------
# Footer
# ----------------------------
st.markdown('<div class="footer">Made with ❤️ — Weather Forecast · © 2025</div>', unsafe_allow_html=True)
