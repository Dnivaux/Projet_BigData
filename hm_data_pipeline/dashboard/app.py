import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import os

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="H&M Data Dashboard", layout="wide")

st.title("H&M Data Platform - Datamart Dashboard")

# Auto-login to the API in the background (no UI)
if 'token' not in st.session_state:
    try:
        response = requests.post(f"{API_URL}/token", data={"username": "admin", "password": "admin"})
        if response.status_code == 200:
            st.session_state['token'] = response.json()["access_token"]
        else:
            st.error("Failed to auto-login to API: Invalid credentials")
            st.session_state['token'] = None
    except Exception as e:
        st.error(f"Failed to connect to API at {API_URL}: {e}")
        st.session_state['token'] = None

if st.session_state.get('token'):
    st.header("Datamart Analysis: Top Articles by Age Group")
    
    # Fetch Data
    headers = {"Authorization": f"Bearer {st.session_state['token']}"}
    limit = st.slider("Limit rows (Pagination)", 10, 500, 100)
    
    @st.cache_data(ttl=60)
    def load_data(limit):
        try:
            res = requests.get(f"{API_URL}/datamart/top_articles?skip=0&limit={limit}", headers=headers)
            if res.status_code == 200:
                return pd.DataFrame(res.json()["data"])
            elif res.status_code == 500:
                st.warning("Table not found or API Error. Did you run the Spark Datamart job?")
                return pd.DataFrame()
            else:
                st.error(f"API Error {res.status_code}: {res.text}")
                return pd.DataFrame()
        except Exception as e:
            st.error(f"Request failed: {e}")
            return pd.DataFrame()
    
    df = load_data(limit)
    
    if not df.empty:
        st.dataframe(df)

        col1, col2 = st.columns(2)
        
        # Graph 1: Distribution of purchases per Age Group
        with col1:
            st.subheader("1. Purchase Count per Age Group")
            agg_age = df.groupby('age_group')['purchase_count'].sum().reset_index()
            fig1 = px.bar(agg_age, x='age_group', y='purchase_count', title="Purchases Distribution by Age Group", labels={"age_group": "Age Group", "purchase_count": "Purchases"})
            st.plotly_chart(fig1, use_container_width=True)

        # Graph 2: Top Product Groups overall
        with col2:
            st.subheader("2. Popular Product Groups")
            agg_prod = df.groupby('product_group_name')['purchase_count'].sum().reset_index().sort_values('purchase_count', ascending=False)
            fig2 = px.pie(agg_prod, names='product_group_name', values='purchase_count', title="Share of Product Groups")
            st.plotly_chart(fig2, use_container_width=True)
            
        # Graph 3: Scatter of Rank vs Purchase Count colored by Age Group
        st.subheader("3. Rank vs Purchase Count by Age Group")
        fig3 = px.scatter(df, x='rank', y='purchase_count', color='age_group', size='purchase_count', hover_data=['article_id', 'product_group_name'], title="Article Rank relative to Purchases")
        st.plotly_chart(fig3, use_container_width=True)
        
else:
    st.warning("Could not auto-login to API. Is the API container running?")
