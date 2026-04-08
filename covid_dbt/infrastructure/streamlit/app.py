import plotly.express as px
import streamlit as st
from snowflake.snowpark.context import get_active_session

# UI CONFIGURATION
st.set_page_config(page_title="COVID-19 Surveillance Dashboard", layout="wide")

# Theme colors from the feature branch
SAND = "#f6f4ee"    # Light Background
INK = "#102a43"     # Dark Text

st.markdown(f"""
    <style>
    /* Main app background */
    .stApp {{
        background-color: {SAND};
    }}

    /* Global text colors for headers and paragraphs */
    .stApp h1, .stApp h2, .stApp h3, .stApp p, .stApp span {{
        color: {INK} !important;
    }}

    /* Metric Card Styling */
    [data-testid="stMetric"] {{
        background-color: #ffffff !important;
        padding: 1.5rem !important;
        border-radius: 0.75rem !important;
        border: 1px solid #e2e8f0 !important;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1) !important;
    }}

    /* Ensure the metric labels/values specifically use the dark text */
    [data-testid="stMetricLabel"], [data-testid="stMetricValue"] {{
        color: {INK} !important;
    }}

    </style>
""", unsafe_allow_html=True)

#Starting the Snowflake session
session = get_active_session()

# 1. Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Choose a Dataset", ["COVID-19 Deaths", "COVID by Census"])

if page == "COVID-19 Deaths":
    # --- PAGE 1: COVID-19 DATA ---
    deaths_per_case = session.table("COVID19_DB.MARTS.FCT_DEATHS_PER_CASES").to_pandas()
    st.title("US COVID-19 Deaths Per Case")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Cases", f"{int(deaths_per_case['TOTAL_RECORDED_CASES'].sum()):,}")
    m2.metric("Total Deaths", f"{int(deaths_per_case['DEATH_COUNT'].sum()):,}")
    m3.metric("Hospitalizations", f"{int(deaths_per_case['HOSPITALIZATION_COUNT'].sum()):,}")
    m4.metric("Avg Deaths per Case", f"{deaths_per_case['DEATHS_PER_CASE_PCT'].mean():.2f}%")

    col1, col2 = st.columns(2)
    with col1:
        # --- TOP 10 total cases PLOT ---
        st.subheader("Top 10 States by Cases")
        # Sort descending and take top 10
        top_10_total_cases = deaths_per_case.sort_values("TOTAL_RECORDED_CASES", ascending=False).head(10)

        fig_top_total_cases = px.bar(
            top_10_total_cases,
            x="RESIDENT_STATE",
            y="TOTAL_RECORDED_CASES",
            labels={"RESIDENT_STATE": "State", "TOTAL_RECORDED_CASES": "Total Cases"},
            color="TOTAL_RECORDED_CASES",
            color_continuous_scale="Reds"
        )
        st.plotly_chart(fig_top_total_cases, use_container_width=True)

        # --- TOP 10 Deaths per Case PLOT ---
        st.subheader("Top 10 States by Deaths per Case")
        # Sort descending and take top 10
        top_10_deaths_per_case = deaths_per_case.sort_values("DEATHS_PER_CASE_PCT", ascending=False).head(10)

        fig_top_deaths_per_case = px.bar(
            top_10_deaths_per_case,
            x="RESIDENT_STATE",
            y="DEATHS_PER_CASE_PCT",
            labels={"RESIDENT_STATE": "State", "DEATHS_PER_CASE_PCT": "Deaths per Case (%)"},
            color="DEATHS_PER_CASE_PCT",
            color_continuous_scale="Reds"
        )
        st.plotly_chart(fig_top_deaths_per_case, use_container_width=True)

    with col2:
        # --- BOTTOM 10 total cases PLOT ---
        st.subheader("Bottom 10 States by Cases")
        # Sort ascending and take bottom 10
        bottom_10_total_cases = deaths_per_case.sort_values("TOTAL_RECORDED_CASES", ascending=False).tail(10)

        fig_bottom_total_cases = px.bar(
            bottom_10_total_cases,
            x="RESIDENT_STATE",
            y="TOTAL_RECORDED_CASES",
            labels={"RESIDENT_STATE": "State", "TOTAL_RECORDED_CASES": "Total Cases"},
            color="TOTAL_RECORDED_CASES",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig_bottom_total_cases, use_container_width=True)

        # --- BOTTOM 10 Deaths per Case PLOT ---
        st.subheader("Bottom 10 States by Deaths per Case")
        # Sort ascending and take bottom 10
        bottom_10_deaths_per_case = deaths_per_case.sort_values("DEATHS_PER_CASE_PCT", ascending=False).tail(10)

        fig_bottom_deaths_per_case = px.bar(
            bottom_10_deaths_per_case,
            x="RESIDENT_STATE",
            y="DEATHS_PER_CASE_PCT",
            labels={"RESIDENT_STATE": "State", "DEATHS_PER_CASE_PCT": "Deaths per Case (%)"},
            color="DEATHS_PER_CASE_PCT",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig_bottom_deaths_per_case, use_container_width=True)

    # Display the raw data table
    st.subheader("Data View")
    st.write(deaths_per_case)

elif page == "COVID by Census":
    covid_by_fips = session.table("COVID19_DB.MARTS.FCT_COVID_BY_FIPS").to_pandas()
    st.title("COVID-19 Cases by Census Data")

    col1, col2 = st.columns(2)
    with col1:
        # --- TOP 10 Cases per Population PLOT ---
        st.subheader("Top 10 States by Cases per Population")
        top_10_cases_per_pop = covid_by_fips.sort_values("CASES_PER_POPULATION_PCT", ascending=False).head(10)

        fig_top_cases_per_pop = px.bar(
            top_10_cases_per_pop,
            x="RESIDENT_STATE",
            y="CASES_PER_POPULATION_PCT",
            labels={"RESIDENT_STATE": "State", "CASES_PER_POPULATION_PCT": "Cases per Population (%)"},
            color="CASES_PER_POPULATION_PCT",
            color_continuous_scale="Greens"
        )
        st.plotly_chart(fig_top_cases_per_pop, use_container_width=True)

    with col2:
        # --- BOTTOM 10 Cases per Population PLOT ---
        st.subheader("Bottom 10 States by Cases per Population")
        bottom_10_cases_per_pop = covid_by_fips.sort_values("CASES_PER_POPULATION_PCT", ascending=False).tail(10)
        fig_bottom_cases_per_pop = px.bar(
            bottom_10_cases_per_pop,
            x="RESIDENT_STATE",
            y="CASES_PER_POPULATION_PCT",
            labels={"RESIDENT_STATE": "State", "CASES_PER_POPULATION_PCT": "Cases per Population (%)"},
            color="CASES_PER_POPULATION_PCT",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig_bottom_cases_per_pop, use_container_width=True)


    # Display the raw data table
    st.subheader("Data View")
    st.write(covid_by_fips)
