import streamlit as st
import pandas as pd

from data_loader import load_all_data

from charts import (
    create_depreciation_chart,
    create_velocity_chart,
    create_regional_price_chart,
    create_variance_chart
)


# =========================================================
# PAGE CONFIGURATION
# =========================================================

st.set_page_config(
    page_title="ResellRadar Dashboard",
    page_icon="📊",
    layout="wide"
)


# =========================================================
# TITLE
# =========================================================

st.title("📊 ResellRadar")

st.subheader(
    "Resale Market Intelligence Dashboard"
)

st.write(
    "Analyze product depreciation, resale velocity "
    "and regional price variations."
)


# =========================================================
# LOAD DATA
# =========================================================

(
    depreciation_df,
    velocity_df,
    regional_df,
    demo_status
) = load_all_data()


# =========================================================
# DEMO DATA WARNING
# =========================================================

using_demo_data = any(
    demo_status.values()
)

if using_demo_data:

    st.warning(
        "⚠️ Demo data is currently being displayed. "
        "The actual curated Parquet files from Person 2 "
        "have not been detected yet."
    )


# =========================================================
# SIDEBAR
# =========================================================

st.sidebar.header("Dashboard Filters")


# Get products from depreciation data
products = sorted(
    depreciation_df["product"]
    .dropna()
    .unique()
    .tolist()
)


selected_product = st.sidebar.selectbox(
    "Select Product",
    ["All"] + products
)


# =========================================================
# FILTER DATA
# =========================================================

if selected_product != "All":

    selected_depreciation = depreciation_df[
        depreciation_df["product"] == selected_product
    ]

else:

    selected_depreciation = depreciation_df


# =========================================================
# KPI SECTION
# =========================================================

st.markdown("## 📈 Market Overview")


col1, col2, col3, col4 = st.columns(4)


# Number of products
with col1:

    product_count = (
        depreciation_df["product"]
        .nunique()
    )

    st.metric(
        "Products",
        product_count
    )


# Average resale days
with col2:

    avg_days = (
        velocity_df["avg_days_to_sell"]
        .mean()
    )

    st.metric(
        "Avg. Days to Sell",
        f"{avg_days:.1f}"
    )


# Number of regions
with col3:

    region_count = (
        regional_df["region"]
        .nunique()
    )

    st.metric(
        "Regions",
        region_count
    )


# Average price
with col4:

    avg_price = (
        regional_df["avg_price"]
        .mean()
    )

    st.metric(
        "Avg. Resale Price",
        f"₹{avg_price:,.0f}"
    )


# =========================================================
# DEPRECIATION GRAPH
# =========================================================

st.markdown("---")

st.header("📉 Product Depreciation")


st.write(
    "This graph shows how the resale price changes "
    "as the product becomes older."
)


depreciation_chart = create_depreciation_chart(
    selected_depreciation
)


st.plotly_chart(
    depreciation_chart,
    use_container_width=True
)


# =========================================================
# RESALE VELOCITY GRAPH
# =========================================================

st.markdown("---")

st.header("⚡ Resale Velocity")


st.write(
    "Lower average days to sell indicates that "
    "the product listings are selling in fewer days."
)


velocity_chart = create_velocity_chart(
    velocity_df
)


st.plotly_chart(
    velocity_chart,
    use_container_width=True
)


# =========================================================
# REGIONAL PRICE GRAPH
# =========================================================

st.markdown("---")

st.header("🌍 Regional Price Comparison")


st.write(
    "Compare average resale prices across different regions."
)


regional_chart = create_regional_price_chart(
    regional_df,
    selected_product
)


st.plotly_chart(
    regional_chart,
    use_container_width=True
)


# =========================================================
# PRICE VARIANCE GRAPH
# =========================================================

st.markdown("---")

st.header("📊 Regional Price Variance")


st.write(
    "This graph shows how much resale prices vary "
    "between regions."
)


variance_chart = create_variance_chart(
    regional_df,
    selected_product
)


st.plotly_chart(
    variance_chart,
    use_container_width=True
)


# =========================================================
# DATA PREVIEW
# =========================================================

st.markdown("---")

st.header("📋 Dataset Preview")


tab1, tab2, tab3 = st.tabs(
    [
        "Depreciation",
        "Resale Velocity",
        "Regional Prices"
    ]
)


with tab1:

    st.dataframe(
        depreciation_df,
        use_container_width=True
    )


with tab2:

    st.dataframe(
        velocity_df,
        use_container_width=True
    )


with tab3:

    st.dataframe(
        regional_df,
        use_container_width=True
    )


# =========================================================
# FOOTER
# =========================================================

st.markdown("---")

st.caption(
    "ResellRadar | Graph & Streamlit Dashboard"
)
