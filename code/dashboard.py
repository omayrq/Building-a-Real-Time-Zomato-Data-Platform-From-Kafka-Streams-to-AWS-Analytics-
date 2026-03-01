import streamlit as st
import pandas as pd
import s3fs
import plotly.express as px

# CONFIG
BUCKET_NAME = "zomato-data-platform"
PROCESSED_PATH = f"s3://{BUCKET_NAME}/processed/orders/"

st.set_page_config(page_title="Zomato Real-Time Analytics", layout="wide")

@st.cache_data
def load_data():
    fs = s3fs.S3FileSystem()
    # Get the latest parquet file
    files = fs.glob(f"{PROCESSED_PATH}*.parquet")
    if not files:
        return pd.DataFrame()
    
    # Load the most recent file
    latest_file = max(files, key=lambda x: fs.info(x)['LastModified'])
    return pd.read_parquet(f"s3://{latest_file}")

# --- DASHBOARD UI ---
st.title("🍕 Zomato ETL: Real-Time Order Insights")
st.markdown("This dashboard pulls processed data directly from the **AWS S3 Silver Layer**.")

df = load_data()

if df.empty:
    st.warning("No processed data found. Run your transformation script first!")
else:
    # 1. Top Level Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Orders", len(df))
    col2.metric("Total Revenue", f"${df['amount'].sum():,.2f}")
    col3.metric("Avg Order Value", f"${df['amount'].mean():,.2f}")

    st.divider()

    # 2. Charts
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.subheader("Orders by Status")
        fig_status = px.pie(df, names='status', hole=0.4, color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig_status, use_container_width=True)

    with chart_col2:
        st.subheader("Revenue Trend")
        # Ensure time is sorted
        df_sorted = df.sort_values('order_time')
        fig_revenue = px.line(df_sorted, x='order_time', y='amount', title="Revenue over Time")
        st.plotly_chart(fig_revenue, use_container_width=True)

    # 3. Raw Data Preview
    st.subheader("Latest Processed Records")
    st.dataframe(df.tail(10), use_container_width=True)