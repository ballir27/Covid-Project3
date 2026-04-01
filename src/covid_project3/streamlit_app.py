import pandas as pd
import plotly.express as px
import snowflake.connector
import streamlit as st
from snowflake.connector import errors as snowflake_errors

from covid_project3.config import config


ACCENT = "#0f766e"
INK = "#102a43"
SAND = "#f6f4ee"


def get_connection():
    return snowflake.connector.connect(
        user=config["SNOWFLAKE_USER"],
        password=config["SNOWFLAKE_PASSWORD"],
        account=config["SNOWFLAKE_ACCOUNT"],
        warehouse=config["SNOWFLAKE_WAREHOUSE"],
        database=config["SNOWFLAKE_DATABASE"],
        schema=config["SNOWFLAKE_SCHEMA"],
    )


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetch_pandas_all()
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_mart_data() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            d.RESIDENT_STATE,
            c.STATE_FIPS,
            c.TOTAL_POPULATION,
            d.TOTAL_RECORDED_CASES,
            d.HOSPITALIZATION_COUNT,
            d.DEATH_COUNT,
            d.DEATHS_PER_CASE,
            d.DEATHS_PER_HOSPITALIZATION,
            c.CASES_PER_100K,
            c.DEATHS_PER_100K,
            c.HOSPITALIZATIONS_PER_100K
        FROM MARTS.FCT_DEATHS_PER_CASES d
        LEFT JOIN MARTS.FCT_CASES_PER_100K_BY_STATE c
            ON d.RESIDENT_STATE = c.RESIDENT_STATE
        ORDER BY TOTAL_RECORDED_CASES DESC
        """
    )


@st.cache_data(ttl=300)
def load_monthly_trends() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            CASE_MONTH,
            RESIDENT_STATE,
            TOTAL_RECORDED_CASES AS TOTAL_CASES,
            DEATH_COUNT AS TOTAL_DEATHS,
            HOSPITALIZATION_COUNT AS TOTAL_HOSPITALIZATIONS,
            CASES_3_MONTH_ROLLING_AVG,
            DEATHS_3_MONTH_ROLLING_AVG,
            HOSPITALIZATIONS_3_MONTH_ROLLING_AVG
        FROM MARTS.FCT_MONTHLY_CASE_TRENDS
        ORDER BY CASE_MONTH, RESIDENT_STATE
        """
    )


@st.cache_data(ttl=300)
def load_age_breakdown() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            RESIDENT_STATE,
            AGE_GROUP,
            COUNT(*) AS TOTAL_CASES,
            COUNT_IF(DEATH_STATUS = 'Yes') AS TOTAL_DEATHS,
            COUNT_IF(HOSPITALIZATION_STATUS = 'Yes')
                AS TOTAL_HOSPITALIZATIONS
        FROM STAGING.STG_CDC_CASES
        WHERE AGE_GROUP IS NOT NULL
        GROUP BY RESIDENT_STATE, AGE_GROUP
        ORDER BY TOTAL_CASES DESC
        """
    )


@st.cache_data(ttl=300)
def load_gender_breakdown() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            RESIDENT_STATE,
            GENDER,
            COUNT(*) AS TOTAL_CASES,
            COUNT_IF(DEATH_STATUS = 'Yes') AS TOTAL_DEATHS,
            COUNT_IF(HOSPITALIZATION_STATUS = 'Yes')
                AS TOTAL_HOSPITALIZATIONS
        FROM STAGING.STG_CDC_CASES
        WHERE GENDER IS NOT NULL
        GROUP BY RESIDENT_STATE, GENDER
        ORDER BY TOTAL_CASES DESC
        """
    )


@st.cache_data(ttl=300)
def load_status_data() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            SOURCE_NAME,
            ROW_COUNT,
            LATEST_BUSINESS_DATE,
            LAST_REFRESHED_AT,
            DAYS_SINCE_LATEST_BUSINESS_DATE,
            HOURS_SINCE_LAST_REFRESH
        FROM MARTS.OPS_SOURCE_FRESHNESS
        ORDER BY SOURCE_NAME
        """
    )


def build_mom_frame(trend_data: pd.DataFrame) -> pd.DataFrame:
    monthly_rollup = (
        trend_data.groupby("CASE_MONTH", as_index=False)[
            ["TOTAL_CASES", "TOTAL_DEATHS", "TOTAL_HOSPITALIZATIONS"]
        ]
        .sum()
        .sort_values("CASE_MONTH")
        .reset_index(drop=True)
    )

    if monthly_rollup.empty:
        return monthly_rollup

    monthly_rollup["CASES_DELTA"] = monthly_rollup["TOTAL_CASES"].diff()
    monthly_rollup["DEATHS_DELTA"] = monthly_rollup["TOTAL_DEATHS"].diff()
    monthly_rollup["HOSPITALIZATIONS_DELTA"] = monthly_rollup[
        "TOTAL_HOSPITALIZATIONS"
    ].diff()

    # Ensure rolling-average columns always exist for downstream charts.
    monthly_rollup["CASES_3_MONTH_ROLLING_AVG"] = (
        monthly_rollup["TOTAL_CASES"].rolling(window=3, min_periods=1).mean()
    )
    monthly_rollup["DEATHS_3_MONTH_ROLLING_AVG"] = (
        monthly_rollup["TOTAL_DEATHS"].rolling(window=3, min_periods=1).mean()
    )
    monthly_rollup["HOSPITALIZATIONS_3_MONTH_ROLLING_AVG"] = (
        monthly_rollup["TOTAL_HOSPITALIZATIONS"]
        .rolling(window=3, min_periods=1)
        .mean()
    )
    return monthly_rollup


def format_delta(value: float) -> str:
    if pd.isna(value):
        return "n/a"
    sign = "+" if value > 0 else ""
    return f"{sign}{int(value):,} vs prior month"


def build_geo_frame(mart_data: pd.DataFrame) -> pd.DataFrame:
    geo_data = mart_data.copy()
    geo_data["STATE_CODE"] = geo_data["RESIDENT_STATE"].str.upper()
    geo_data["DEATH_RATE_PCT"] = geo_data["DEATHS_PER_CASE"] * 100
    geo_data["HOSPITALIZATION_RATE_PCT"] = (
        geo_data["HOSPITALIZATION_COUNT"]
        / geo_data["TOTAL_RECORDED_CASES"].clip(lower=1)
        * 100
    )
    return geo_data


def build_geo_figure(geo_data: pd.DataFrame, color_metric: str):
    label_map = {
        "DEATH_RATE_PCT": "Deaths per case (%)",
        "HOSPITALIZATION_RATE_PCT": "Hospitalizations per case (%)",
        "TOTAL_RECORDED_CASES": "Recorded cases",
        "DEATH_COUNT": "Deaths",
        "CASES_PER_100K": "Cases per 100k",
        "DEATHS_PER_100K": "Deaths per 100k",
        "HOSPITALIZATIONS_PER_100K": "Hospitalizations per 100k",
    }
    fig = px.choropleth(
        geo_data,
        locations="STATE_CODE",
        locationmode="USA-states",
        scope="usa",
        color=color_metric,
        hover_name="RESIDENT_STATE",
        hover_data={
            "TOTAL_RECORDED_CASES": ":,",
            "HOSPITALIZATION_COUNT": ":,",
            "DEATH_COUNT": ":,",
            "DEATH_RATE_PCT": ":.2f",
            "HOSPITALIZATION_RATE_PCT": ":.2f",
            "STATE_CODE": False,
        },
        color_continuous_scale="YlOrRd",
        labels={color_metric: label_map[color_metric]},
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def apply_styles():
    st.markdown(
        f"""
        <style>
        .stApp {{
            background:
                radial-gradient(
                    circle at top left,
                    #f7d7b5 0%,
                    rgba(247, 215, 181, 0) 28%
                ),
                linear-gradient(180deg, {SAND} 0%, #ffffff 100%);
        }}
        .block-container {{
            padding-top: 2rem;
            padding-bottom: 3rem;
        }}
        .hero {{
            padding: 1.4rem 1.6rem;
            border-radius: 20px;
            background: linear-gradient(135deg, {INK} 0%, #1d4e89 100%);
            color: white;
            margin-bottom: 1.2rem;
            box-shadow: 0 18px 50px rgba(16, 42, 67, 0.18);
        }}
        .hero h1 {{
            font-size: 2.1rem;
            margin: 0 0 0.3rem 0;
        }}
        .hero p {{
            margin: 0;
            color: rgba(255, 255, 255, 0.86);
        }}
        .label-chip {{
            display: inline-block;
            margin-top: 0.75rem;
            padding: 0.25rem 0.65rem;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.14);
            font-size: 0.82rem;
            letter-spacing: 0.03em;
        }}
        div[data-testid="stMetric"] {{
            background: rgba(255, 255, 255, 0.78);
            border: 1px solid rgba(15, 118, 110, 0.12);
            border-radius: 18px;
            padding: 0.8rem 1rem;
            box-shadow: 0 12px 30px rgba(15, 23, 42, 0.05);
        }}
        div[data-testid="stMetricLabel"] p {{
            color: #486581 !important;
        }}
        div[data-testid="stMetricValue"] {{
            color: #102a43 !important;
        }}
        div[data-testid="stMetricDelta"] {{
            color: #0f766e !important;
        }}
        .section-note {{
            color: #486581;
            font-size: 0.95rem;
            margin-bottom: 0.75rem;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def safe_numeric_sum(series: pd.Series) -> int:
    return int(pd.to_numeric(series, errors="coerce").fillna(0).sum())


def load_or_error(loader):
    try:
        return loader(), None
    except snowflake_errors.Error as exc:
        return pd.DataFrame(), str(exc)


st.set_page_config(page_title="COVID Analytics", layout="wide")
apply_styles()

st.markdown(
    """
    <div class="hero">
      <h1>COVID Outcomes Observatory</h1>
            <p>
                Explore state-level severity, trend movement, and demographic
                pressure points from your Snowflake warehouse.
            </p>
      <div class="label-chip">Snowflake + dbt + Airflow + Streamlit</div>
    </div>
    """,
    unsafe_allow_html=True,
)

mart_df, mart_error = load_or_error(load_mart_data)
trend_df, trend_error = load_or_error(load_monthly_trends)
age_df, age_error = load_or_error(load_age_breakdown)
gender_df, gender_error = load_or_error(load_gender_breakdown)
status_df, status_error = load_or_error(load_status_data)

if mart_error:
    st.error("Mart data is not available yet.")
    st.code(mart_error)
    st.stop()

states = sorted(state for state in mart_df["RESIDENT_STATE"].dropna().unique())

st.sidebar.header("Filters")
selected_states = st.sidebar.multiselect(
    "States",
    options=states,
    default=states[:10] if len(states) > 10 else states,
)
min_cases = st.sidebar.slider(
    "Minimum recorded cases",
    min_value=0,
    max_value=int(mart_df["TOTAL_RECORDED_CASES"].max()),
    value=0,
    step=max(1, int(mart_df["TOTAL_RECORDED_CASES"].max() / 25)),
)
sort_metric = st.sidebar.selectbox(
    "Rank states by",
    options=[
        "DEATHS_PER_CASE",
        "DEATHS_PER_HOSPITALIZATION",
        "TOTAL_RECORDED_CASES",
        "DEATH_COUNT",
        "HOSPITALIZATION_COUNT",
    ],
)
top_n = st.sidebar.slider(
    "States to chart", min_value=5, max_value=50, value=15
)
selected_trend_state = st.sidebar.selectbox(
    "Trend focus",
    options=["All selected states", *states],
)
map_metric = st.sidebar.selectbox(
    "Map color metric",
    options=[
        "DEATH_RATE_PCT",
        "HOSPITALIZATION_RATE_PCT",
        "TOTAL_RECORDED_CASES",
        "DEATH_COUNT",
        "CASES_PER_100K",
        "DEATHS_PER_100K",
        "HOSPITALIZATIONS_PER_100K",
    ],
)

filtered_mart = mart_df.copy()
if selected_states:
    filtered_mart = filtered_mart[
        filtered_mart["RESIDENT_STATE"].isin(selected_states)
    ]
filtered_mart = filtered_mart[
    filtered_mart["TOTAL_RECORDED_CASES"] >= min_cases
].sort_values(sort_metric, ascending=False)

if filtered_mart.empty:
    st.warning("No states match the current filters.")
    st.stop()

trend_view = trend_df.copy()
if selected_states:
    trend_view = trend_view[trend_view["RESIDENT_STATE"].isin(selected_states)]

if selected_trend_state != "All selected states":
    trend_view = trend_view[
        trend_view["RESIDENT_STATE"] == selected_trend_state
    ]

monthly_summary = pd.DataFrame()
if not trend_error:
    monthly_summary = build_mom_frame(trend_view)

latest_snapshot = pd.Series(dtype="object")
if not monthly_summary.empty:
    latest_snapshot = monthly_summary.iloc[-1]

(
    overview_tab,
    trends_tab,
    demographic_tab,
    geography_tab,
    pipeline_tab,
) = st.tabs(
    ["Overview", "Trends", "Demographics", "Geography", "Pipeline"]
)

with overview_tab:
    st.markdown(
        (
            '<div class="section-note">State-level severity metrics '
            'from the mart layer.</div>'
        ),
        unsafe_allow_html=True,
    )

    metric_cols = st.columns(4)
    metric_cols[0].metric("States in view", f"{len(filtered_mart):,}")
    metric_cols[1].metric(
        "Recorded cases",
        f"{safe_numeric_sum(filtered_mart['TOTAL_RECORDED_CASES']):,}",
    )
    metric_cols[2].metric(
        "Hospitalizations",
        f"{safe_numeric_sum(filtered_mart['HOSPITALIZATION_COUNT']):,}",
    )
    metric_cols[3].metric(
        "Deaths", f"{safe_numeric_sum(filtered_mart['DEATH_COUNT']):,}"
    )

    st.subheader("Month-over-Month KPI Snapshot")
    if trend_error:
        st.warning("Trend data is unavailable.")
        st.code(trend_error)
    elif monthly_summary.empty:
        st.info("No monthly trend rows available for the current filter set.")
    else:
        scope_label = selected_trend_state
        if selected_trend_state == "All selected states":
            scope_label = "Selected states"
        st.caption(
            f"Latest month for {scope_label}: {latest_snapshot['CASE_MONTH']}"
        )
        delta_cols = st.columns(3)
        delta_cols[0].metric(
            "Cases",
            f"{int(latest_snapshot['TOTAL_CASES']):,}",
            format_delta(latest_snapshot["CASES_DELTA"]),
        )
        delta_cols[1].metric(
            "Deaths",
            f"{int(latest_snapshot['TOTAL_DEATHS']):,}",
            format_delta(latest_snapshot["DEATHS_DELTA"]),
        )
        delta_cols[2].metric(
            "Hospitalizations",
            f"{int(latest_snapshot['TOTAL_HOSPITALIZATIONS']):,}",
            format_delta(latest_snapshot["HOSPITALIZATIONS_DELTA"]),
        )

    top_states = filtered_mart.head(top_n).copy()

    chart_left, chart_right = st.columns([1.2, 1])
    with chart_left:
        st.subheader("State Ranking")
        st.bar_chart(top_states.set_index("RESIDENT_STATE")[[sort_metric]])
    with chart_right:
        st.subheader("Severity Relationship")
        scatter_df = top_states[
            [
                "RESIDENT_STATE",
                "HOSPITALIZATION_COUNT",
                "DEATH_COUNT",
                "TOTAL_RECORDED_CASES",
            ]
        ].copy()
        scatter_df["bubble_size"] = scatter_df[
            "TOTAL_RECORDED_CASES"
        ].clip(lower=1)
        st.scatter_chart(
            scatter_df,
            x="HOSPITALIZATION_COUNT",
            y="DEATH_COUNT",
            size="bubble_size",
            color=None,
        )

    st.subheader("Filtered Mart Table")
    st.dataframe(top_states, use_container_width=True)

with trends_tab:
    st.markdown(
        (
            '<div class="section-note">Monthly movement from the '
            'staging layer to show timing rather than just rolled-up '
            'totals.</div>'
        ),
        unsafe_allow_html=True,
    )

    if trend_error:
        st.warning("Trend data is unavailable.")
        st.code(trend_error)
    else:
        trend_cols = st.columns(2)
        with trend_cols[0]:
            st.subheader("Cases vs Deaths Over Time")
            st.line_chart(
                monthly_summary.set_index("CASE_MONTH")[
                    ["TOTAL_CASES", "TOTAL_DEATHS"]
                ]
            )
        with trend_cols[1]:
            st.subheader("Hospitalizations Over Time")
            st.area_chart(
                monthly_summary.set_index("CASE_MONTH")[
                    ["TOTAL_HOSPITALIZATIONS"]
                ]
            )

        st.subheader("Rolling Averages")
        st.line_chart(
            monthly_summary.set_index("CASE_MONTH")[
                [
                    "CASES_3_MONTH_ROLLING_AVG",
                    "DEATHS_3_MONTH_ROLLING_AVG",
                    "HOSPITALIZATIONS_3_MONTH_ROLLING_AVG",
                ]
            ]
        )

        latest_window = monthly_summary.tail(6).copy()
        st.subheader("Recent Six-Month Snapshot")
        st.dataframe(latest_window, use_container_width=True)

        if len(monthly_summary) > 1:
            st.subheader("Month-over-Month Change Table")
            delta_table = monthly_summary[
                [
                    "CASE_MONTH",
                    "TOTAL_CASES",
                    "CASES_DELTA",
                    "TOTAL_DEATHS",
                    "DEATHS_DELTA",
                    "TOTAL_HOSPITALIZATIONS",
                    "HOSPITALIZATIONS_DELTA",
                ]
            ].tail(12)
            st.dataframe(delta_table, use_container_width=True)

with demographic_tab:
    st.markdown(
        (
            '<div class="section-note">Demographic breakdowns come '
            'directly from the staging model so they stay close to '
            'the source grain.</div>'
        ),
        unsafe_allow_html=True,
    )

    demo_left, demo_right = st.columns(2)

    with demo_left:
        st.subheader("Age Group Distribution")
        if age_error:
            st.warning("Age data is unavailable.")
            st.code(age_error)
        else:
            age_view = age_df.copy()
            if selected_states:
                age_view = age_view[
                    age_view["RESIDENT_STATE"].isin(selected_states)
                ]
            age_summary = (
                age_view.groupby("AGE_GROUP", as_index=False)["TOTAL_CASES"]
                .sum()
                .sort_values("TOTAL_CASES", ascending=False)
            )
            st.bar_chart(age_summary.set_index("AGE_GROUP")[["TOTAL_CASES"]])
            st.dataframe(age_summary.head(12), use_container_width=True)

    with demo_right:
        st.subheader("Gender Outcome Mix")
        if gender_error:
            st.warning("Gender data is unavailable.")
            st.code(gender_error)
        else:
            gender_view = gender_df.copy()
            if selected_states:
                gender_view = gender_view[
                    gender_view["RESIDENT_STATE"].isin(selected_states)
                ]
            gender_summary = (
                gender_view.groupby("GENDER", as_index=False)[
                    ["TOTAL_CASES", "TOTAL_DEATHS", "TOTAL_HOSPITALIZATIONS"]
                ]
                .sum()
                .sort_values("TOTAL_CASES", ascending=False)
            )
            st.bar_chart(
                gender_summary.set_index("GENDER")[
                    [
                        "TOTAL_CASES",
                        "TOTAL_DEATHS",
                        "TOTAL_HOSPITALIZATIONS",
                    ]
                ]
            )
            st.dataframe(gender_summary, use_container_width=True)

with geography_tab:
    st.markdown(
        (
            '<div class="section-note">Choropleth-style state mapping '
            'for case load and severity. County-level geometry is not '
            'modeled yet, so this view stays at the state grain.</div>'
        ),
        unsafe_allow_html=True,
    )

    geography_df = build_geo_frame(filtered_mart)
    st.plotly_chart(
        build_geo_figure(geography_df, map_metric),
        use_container_width=True,
    )

    geo_cols = st.columns([1.1, 0.9])
    with geo_cols[0]:
        st.subheader("Map Metric Ranking")
        st.dataframe(
            geography_df[
                [
                    "RESIDENT_STATE",
                    "TOTAL_RECORDED_CASES",
                    "DEATH_COUNT",
                    "DEATH_RATE_PCT",
                    "HOSPITALIZATION_RATE_PCT",
                    "CASES_PER_100K",
                    "DEATHS_PER_100K",
                    "HOSPITALIZATIONS_PER_100K",
                ]
            ].sort_values(map_metric, ascending=False),
            use_container_width=True,
        )
    with geo_cols[1]:
        st.subheader("Selected State Share")
        share_df = geography_df[
            ["RESIDENT_STATE", "TOTAL_RECORDED_CASES"]
        ].copy()
        share_df = share_df.sort_values(
            "TOTAL_RECORDED_CASES", ascending=False
        ).head(top_n)
        st.bar_chart(
            share_df.set_index("RESIDENT_STATE")[["TOTAL_RECORDED_CASES"]]
        )

with pipeline_tab:
    st.markdown(
        (
            '<div class="section-note">Quick operational view of '
            'warehouse layer row counts to confirm ingestion and '
            'transformations are landing.</div>'
        ),
        unsafe_allow_html=True,
    )

    if status_error:
        st.warning("Could not load warehouse status.")
        st.code(status_error)
    else:
        st.dataframe(status_df, use_container_width=True)
        st.bar_chart(status_df.set_index("SOURCE_NAME")[["ROW_COUNT"]])
