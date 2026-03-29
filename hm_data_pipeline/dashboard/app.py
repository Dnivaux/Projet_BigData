import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import os

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Tableau de Bord H&M", layout="wide")

st.title("Plateforme H&M Data - Tableau de Bord Datamart")

# Auto-login to the API in the background (no UI)
if 'token' not in st.session_state:
    try:
        response = requests.post(f"{API_URL}/token", data={"username": "admin", "password": "admin"})
        if response.status_code == 200:
            st.session_state['token'] = response.json()["access_token"]
        else:
            st.error("Erreur: impossible de se connecter à l'API (identifiants invalides)")
            st.session_state['token'] = None
    except Exception as e:
        st.error(f"Erreur: impossible de se connecter à l'API ({API_URL}): {e}")
        st.session_state['token'] = None

if st.session_state.get('token'):
    st.header("Analyse Datamart : Top Articles par Groupe de Produits et Tranche d'Âge")
    
    # Fetch Data
    headers = {"Authorization": f"Bearer {st.session_state['token']}"}
    limit = st.slider("Nombre de lignes (Pagination)", 10, 500, 100)
    
    @st.cache_data(ttl=60)
    def load_data(limit):
        try:
            res = requests.get(f"{API_URL}/datamart/top_articles?skip=0&limit={limit}", headers=headers)
            if res.status_code == 200:
                return pd.DataFrame(res.json()["data"])
            elif res.status_code == 500:
                st.warning("Erreur: table non trouvée. As-tu exécuté le job Spark Datamart ?")
                return pd.DataFrame()
            else:
                st.error(f"Erreur API {res.status_code}: {res.text}")
                return pd.DataFrame()
        except Exception as e:
            st.error(f"Erreur de requête: {e}")
            return pd.DataFrame()
    
    df = load_data(limit)
    
    if not df.empty:
        st.dataframe(df)

        col1, col2 = st.columns(2)
        
        # Graph 1
        with col1:
            st.subheader("1. Nombre de Achats par Groupe d'Âge")
            agg_age = df.groupby('age_group')['purchase_count'].sum().reset_index()
            fig1 = px.bar(agg_age, x='age_group', y='purchase_count', title="Distribution des Achats par Groupe d'Âge", labels={"age_group": "Groupe d'Âge", "purchase_count": "Achats"})
            st.plotly_chart(fig1, use_container_width=True)

        # Graph 2
        with col2:
            st.subheader("2. Groupe de Produits les Plus Populaires")
            agg_prod = df.groupby('product_group_name')['purchase_count'].sum().reset_index().sort_values('purchase_count', ascending=False)
            fig2 = px.pie(agg_prod, names='product_group_name', values='purchase_count', title="Part des Groupe de Produits")
            st.plotly_chart(fig2, use_container_width=True)
            
        # Graph 3
        st.subheader("3. Rang vs Nombre d'Achats par Groupe d'Âge")
        fig3 = px.scatter(df, x='rank', y='purchase_count', color='age_group', size='purchase_count', hover_data=['article_id', 'product_group_name'], title="Rang des Articles par Rapport aux Achats")
        st.plotly_chart(fig3, use_container_width=True)
        
else:
    st.warning("Erreur: impossible de se connecter à l'API. Le conteneur API tourne-t-il ?")
