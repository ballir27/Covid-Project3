import plotly.express as px
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Get the current Snowflake session
session = get_active_session()

deaths_per_case = session.table("COVID19_DB.MARTS.FCT_DEATHS_PER_CASES").to_pandas()

st.title("US COVID-19 Dashboard")

# --- TOP 10 PLOT ---
st.subheader("Top 10 States by Cases")
# Sort descending and take top 10
top_10 = deaths_per_case.sort_values("TOTAL_RECORDED_CASES", ascending=False).head(10)

fig_top = px.bar(
    top_10,
    x="RESIDENT_STATE",
    y="TOTAL_RECORDED_CASES",
    labels={"RESIDENT_STATE": "State", "TOTAL_RECORDED_CASES": "Total Cases"},
    color="TOTAL_RECORDED_CASES",
    color_continuous_scale="Reds"
)
st.plotly_chart(fig_top, use_container_width=True)

# --- BOTTOM 10 PLOT ---
st.subheader("Bottom 10 States by Cases")
# Sort ascending and take bottom 10
bottom_10 = deaths_per_case.sort_values("TOTAL_RECORDED_CASES", ascending=False).tail(10)

fig_bottom = px.bar(
    bottom_10,
    x="RESIDENT_STATE",
    y="TOTAL_RECORDED_CASES",
    labels={"RESIDENT_STATE": "State", "TOTAL_RECORDED_CASES": "Total Cases"},
    color="TOTAL_RECORDED_CASES",
    color_continuous_scale="Blues" # Changed to Blue to distinguish from Top 10
)
st.plotly_chart(fig_bottom, use_container_width=True)

# Display the raw data table
st.subheader("Data View")
st.write(deaths_per_case)
