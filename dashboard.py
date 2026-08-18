"""
Streamlit dashboard over gold Parquet tables.
"""

import pandas as pd
import plotly.express as px
import streamlit as st

GOLD_DIR = "output/gold"

st.set_page_config(page_title="Target Sales Analytics Platform", layout="wide")

@st.cache_data
def load_gold_table(table_name: str) -> pd.DataFrame:
    return pd.read_parquet(f"{GOLD_DIR}/{table_name}")

st.title("Target Sales Analytics Platform — Dashboard")
st.caption(
    "Key retail performance indicators computed via the pipeline: "
    "Kafka → PySpark → Hive."
)

try:
    city_sales = load_gold_table("city_sales")
    state_sales = load_gold_table("state_sales")
    review_dist = load_gold_table("review_score_distribution")
    delivery_vs_review = load_gold_table("delivery_vs_review")
    corr = load_gold_table("value_freight_qty_correlation")
    timing = load_gold_table("delivery_timing_summary")
except FileNotFoundError:
    st.error(
        "Gold tables not found in output/gold/. Please run the pipeline first:\n\n"
        "    ./run_pipeline.sh --no-kafka\n\n"
    )
    st.stop()

# Key Performance Indicators (KPIs) banner
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Completed Orders", f"{int(state_sales['order_count'].sum()):,}")
col2.metric("Gross Revenue", f"${state_sales['total_sales'].sum():,.2f}")
col3.metric("States Serviced", len(state_sales))
col4.metric(
    "Avg Review Score",
    f"{(review_dist['review_score'] * review_dist['num_orders']).sum() / review_dist['num_orders'].sum():.2f}",
)

st.divider()

left_col, right_col = st.columns([3, 2])

with left_col:
    st.subheader("Top 10 Cities by Gross Revenue")
    top_cities = city_sales.sort_values("total_sales", ascending=False).head(10)
    fig_city = px.bar(
        top_cities, x="total_sales", y="customer_city", orientation="h",
        labels={"total_sales": "Total Sales ($)", "customer_city": ""},
    )
    fig_city.update_layout(yaxis={"categoryorder": "total ascending"}, height=420)
    st.plotly_chart(fig_city, use_container_width=True)

with right_col:
    st.subheader("Customer Satisfaction Rating Distribution")
    fig_reviews = px.bar(
        review_dist, x="review_score", y="num_orders",
        labels={"review_score": "Review Score (Stars)", "num_orders": "Order Count"},
    )
    fig_reviews.update_layout(height=420)
    st.plotly_chart(fig_reviews, use_container_width=True)

st.divider()

# Delivery time vs satisfaction analysis
st.subheader("Delivery Duration vs. Customer Satisfaction Score")
st.markdown(
    "Calculated average delivery timeline (days) grouped by review score. "
    "The baseline indicates a relatively uniform distribution across all score brackets, "
    "showing no direct linear correlation between delivery days and rating in this sample size."
)
fig_delivery = px.bar(
    delivery_vs_review, x="review_score", y="avg_delivery_days",
    labels={"review_score": "Review Score (Stars)", "avg_delivery_days": "Avg. Delivery Duration (Days)"},
)
fig_delivery.update_layout(height=350)
st.plotly_chart(fig_delivery, use_container_width=True)

st.divider()

col_corr, col_time = st.columns(2)
with col_corr:
    st.subheader("Attribute Correlations Matrix")
    st.dataframe(corr, hide_index=True, use_container_width=True)
with col_time:
    st.subheader("Delivery & Approval SLAs")
    st.dataframe(timing, hide_index=True, use_container_width=True)

st.divider()
st.caption(
    "Target Sales Analytics Platform — Engineered with Kafka, PySpark, and Hive Metastore. "
    "See README.md for deployment and pipeline architecture details."
)
