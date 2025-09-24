import os
import json
from datetime import datetime, timezone
import streamlit as st
import requests
from slugify import slugify

BACKEND = os.getenv("BACKEND_URL", "http://backend:8000")
TIMEOUT_S = 5

st.set_page_config(page_title="OpenMeteo Streamlit", layout="wide")
st.title("OpenMeteo — Frontend (batch & config)")
st.caption(f"Backend: {BACKEND}")

def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

@st.cache_data(ttl=3)
def get_config():
    try:
        r = requests.get(f"{BACKEND}/config/cities", timeout=TIMEOUT_S)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Echec GET /config/cities : {e}")
        return {"version": 0, "updated_at": "", "updated_by": "", "cities": []}

def upsert_city(payload: dict):
    try:
        r = requests.post(f"{BACKEND}/config/cities", json=payload, headers={"X-User": "streamlit"}, timeout=TIMEOUT_S)
        r.raise_for_status()
        st.success("Ville enregistrée / mise à jour.")
        st.rerun()
    except Exception as e:
        st.error(f"Echec POST /config/cities : {e}")

with st.sidebar:
    st.success("hello sidebar")
    # Ping backend
    try:
        pong = requests.get(f"{BACKEND}/", timeout=TIMEOUT_S).json()
        st.write(":white_check_mark: Backend OK", pong)
    except Exception as e:
        st.write(":x: Backend KO", e)

tab1, tab2 = st.tabs(["Cities", "Queries (hello)"])

with tab1:
    cfg = get_config()
    meta_col, _ = st.columns([2, 3])
    with meta_col:
        st.subheader("Configuration")
        st.json({
            "version": cfg.get("version"),
            "updated_at": cfg.get("updated_at"),
            "updated_by": cfg.get("updated_by"),
        })

    st.markdown("---")
    st.subheader("Ajouter / Mettre à jour une ville")

    with st.form("city_form"):
        c1, c2 = st.columns(2)
        name = c1.text_input("Nom", "Paris")
        country = c2.text_input("Pays (ISO2/3)", "FR", max_chars=3)
        lat = c1.number_input("Latitude", value=48.8566, format="%.6f")
        lon = c2.number_input("Longitude", value=2.3522, format="%.6f")
        active = st.checkbox("Active", value=True)

        # ID par défaut: slug(name-country)
        default_id = slugify(f"{name}-{country}".lower())
        city_id = st.text_input("ID (unique)", value=default_id, help="Ex: paris-fr")

        submit = st.form_submit_button("Enregistrer / Mettre à jour")
        if submit:
            payload = {
                "id": city_id,
                "name": name,
                "country": country.upper(),
                "lat": lat,
                "lon": lon,
                "active": active,
            }
            upsert_city(payload)

    st.markdown("---")
    st.subheader("Villes configurées")
    cities = cfg.get("cities", [])
    if cities:
        # petit tri visuel
        cities_sorted = sorted(cities, key=lambda x: (not x.get("active", True), x.get("name","").lower()))
        st.dataframe(cities_sorted, use_container_width=True, hide_index=True)
    else:
        st.info("Aucune ville pour l’instant.")

with tab2:
    st.write("Queries tab — hello")
    st.code("POST /query -> {'message':'hello query received', ...}")
    st.info("Ici on viendra brancher un déclenchement de batch (Airflow) ou une requête Kafka Q_REQ/Q_RES.")
