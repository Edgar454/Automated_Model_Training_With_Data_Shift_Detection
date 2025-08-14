import streamlit as st

st.set_page_config(page_title="Weather Forecast", layout="centered", page_icon="🌦")
pages = {"Menu": [
        st.Page("weather_app.py", title="Main"),
        st.Page("about.py", title="About"),
        ]}

pg = st.navigation(pages)
pg.run()