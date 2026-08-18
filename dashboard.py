"""
A small read-only dashboard over the pipeline's gold Parquet tables.

Deliberately NOT a Spark app: by the time results reach here they are tiny
aggregated tables (a few hundred rows at most), so pandas + Streamlit is the
right-sized tool. Using Spark to serve a dashboard would be like using a
freight truck to deliver a letter - the earlier stages already did the big
compute, this just displays the answer.

Run with:
    .venv/bin/streamlit run dashboard.py
"""

import pandas as pd
import plotly.express as px
import streamlit as st

GOLD = "output/gold"

st.set_page_config(page_title="Target Sales Pipeline", layout="wide")


@st.cache_data
def load(table: str) -> pd.DataFrame:
    return pd.read_parquet(f"{GOLD}/{table}")


st.title("Target Sales Data Pipeline — Results")
st.caption(
    "Every number below was produced by the pipeline in this repo: "
    "Kafka → PySpark → Hive. This page just reads the gold Parquet tables it wrote."
)

try:
    city_sales = load("city_sales")
    state_sales = load("state_sales")
    review_dist = load("review_score_distribution")
    delivery_vs_review = load("delivery_vs_review")
    corr = load("value_freight_qty_correlation")
    timing = load("delivery_timing_summary")
except FileNotFoundError:
    st.error(
        "No gold tables found in output/gold/. Run the pipeline first:\n\n"
        "    ./run_pipeline.sh --no-kafka\n\n"
        "(or the full ./run_pipeline.sh if you also want the Kafka stage)."
    )
    st.stop()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total orders (delivered)", f"{int(state_sales['order_count'].sum()):,}")
col2.metric("Total sales", f"{state_sales['total_sales'].sum():,.2f}")
col3.metric("States", len(state_sales))
col4.metric(
    "Avg review score",
    f"{(review_dist['review_score'] * review_dist['num_orders']).sum() / review_dist['num_orders'].sum():.2f}",
)

st.divider()

left, right = st.columns([3, 2])

with left:
    st.subheader("Top cities by revenue")
    top_cities = city_sales.sort_values("total_sales", ascending=False).head(10)
    fig = px.bar(
        top_cities, x="total_sales", y="customer_city", orientation="h",
        labels={"total_sales": "Total sales", "customer_city": ""},
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=420)
    st.plotly_chart(fig, width="stretch")

with right:
    st.subheader("Review score distribution")
    fig2 = px.bar(
        review_dist, x="review_score", y="num_orders",
        labels={"review_score": "Stars", "num_orders": "Orders"},
    )
    fig2.update_layout(height=420)
    st.plotly_chart(fig2, width="stretch")

st.divider()

st.subheader("The headline finding: delivery time vs. review score")
st.markdown(
    "The expected story is *slower delivery → angrier customers → worse reviews*. "
    "The pipeline checked this directly — the averages below stay essentially flat "
    "across every delivery-time bucket. See "
    "[`05_insight_deepdive.py`](src/processing/05_insight_deepdive.py) and the README "
    "for the full investigation (non-linear cliff? tail effect? statistical power?) "
    "that ruled out this being a bug."
)
fig3 = px.bar(
    delivery_vs_review, x="review_score", y="avg_delivery_days",
    labels={"review_score": "Review score (stars)", "avg_delivery_days": "Avg. delivery days"},
)
fig3.update_layout(height=350)
st.plotly_chart(fig3, width="stretch")

st.divider()

c1, c2 = st.columns(2)
with c1:
    st.subheader("What correlates with what")
    st.dataframe(corr, hide_index=True, width="stretch")
with c2:
    st.subheader("Delivery & approval timing")
    st.dataframe(timing, hide_index=True, width="stretch")

st.divider()
st.caption(
    "Source: [github.com/your-username/target-sales-pipeline](.) — "
    "Kafka producer/consumer, PySpark cleaning + analysis, Hive metastore. "
    "See README.md for architecture and notes/CONCEPTS.md for the underlying concepts."
)
