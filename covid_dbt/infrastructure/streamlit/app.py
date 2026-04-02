import plotly.express as px
import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

# 1. Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Choose a Dataset", ["COVID-19 Deaths", "COVID by Census"])

if page == "COVID-19 Deaths":
    # --- PAGE 1: COVID-19 DATA ---
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
        color_continuous_scale="Blues"
    )
    st.plotly_chart(fig_bottom, use_container_width=True)

    # --- TOP 10 PLOT ---
    st.subheader("Top 10 States by Deaths per Case")
    # Sort descending and take top 10
    top_10 = deaths_per_case.sort_values("DEATHS_PER_CASE_PCT", ascending=False).head(10)

    fig_top = px.bar(
        top_10,
        x="RESIDENT_STATE",
        y="DEATHS_PER_CASE_PCT",
        labels={"RESIDENT_STATE": "State", "DEATHS_PER_CASE_PCT": "Deaths per Case (%)"},
        color="DEATHS_PER_CASE_PCT",
        color_continuous_scale="Reds"
    )
    st.plotly_chart(fig_top, use_container_width=True)

    # --- BOTTOM 10 PLOT ---
    st.subheader("Bottom 10 States by Deaths per Case")
    # Sort ascending and take bottom 10
    bottom_10 = deaths_per_case.sort_values("DEATHS_PER_CASE_PCT", ascending=False).tail(10)

    fig_bottom = px.bar(
        bottom_10,
        x="RESIDENT_STATE",
        y="DEATHS_PER_CASE_PCT",
        labels={"RESIDENT_STATE": "State", "DEATHS_PER_CASE_PCT": "Deaths per Case (%)"},
        color="DEATHS_PER_CASE_PCT",
        color_continuous_scale="Blues"
    )
    st.plotly_chart(fig_bottom, use_container_width=True)

    # Display the raw data table
    st.subheader("Data View")
    st.write(deaths_per_case)
elif page == "COVID by Census":
    covid_by_fips = session.table("COVID19_DB.MARTS.FCT_COVID_BY_FIPS").to_pandas()
    st.title("COVID-19 Cases by Census Data")

    # --- TOP 10 PLOT ---
    st.subheader("Top 10 States by Cases per Population")
    # Sort descending and take top 10
    top_10 = covid_by_fips.sort_values("CASES_PER_POPULATION_PCT", ascending=False).head(10)

    fig_top = px.bar(
        top_10,
        x="RESIDENT_STATE",
        y="CASES_PER_POPULATION_PCT",
        labels={"RESIDENT_STATE": "State", "CASES_PER_POPULATION_PCT": "Cases per Population (%)"},
        color="CASES_PER_POPULATION_PCT",
        color_continuous_scale="Greens"
    )
    st.plotly_chart(fig_top, use_container_width=True)

    # --- BOTTOM 10 PLOT ---
    st.subheader("Bottom 10 States by Cases per Population")
    # Sort ascending and take bottom 10
    bottom_10 = covid_by_fips.sort_values("CASES_PER_POPULATION_PCT", ascending=False).tail(10)
    fig_bottom = px.bar(
        bottom_10,
        x="RESIDENT_STATE",
        y="CASES_PER_POPULATION_PCT",
        labels={"RESIDENT_STATE": "State", "CASES_PER_POPULATION_PCT": "Cases per Population (%)"},
        color="CASES_PER_POPULATION_PCT",
        color_continuous_scale="Blues"
    )
    st.plotly_chart(fig_bottom, use_container_width=True)


    # Display the raw data table
    st.subheader("Data View")
    st.write(covid_by_fips)
