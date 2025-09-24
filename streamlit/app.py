import os
import json
from datetime import datetime, timezone, date
import requests
import streamlit as st

try:
    from slugify import slugify
except Exception:
    # Fallback simple si slugify n'est pas installé
    def slugify(s: str) -> str:
        return (
            s.lower()
            .replace(" ", "-")
            .replace("_", "-")
            .replace("/", "-")
        )

# ----------------------------
# Configuration générale
# ----------------------------
BACKEND = os.getenv("BACKEND_URL", "http://backend:8000")
TIMEOUT_S = 8

st.set_page_config(page_title="OpenMeteo Streamlit", layout="wide")
st.title("OpenMeteo — Frontend (batch & config)")
st.caption(f"Backend: {BACKEND} • Timestamp: {datetime.utcnow().isoformat()}")

# ----------------------------
# Helpers
# ----------------------------
def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

@st.cache_data(ttl=5)
def get_config() -> dict:
    try:
        r = requests.get(f"{BACKEND}/config/cities", timeout=TIMEOUT_S)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Echec GET /config/cities : {e}")
        return {"version": 0, "updated_at": "", "updated_by": "", "cities": []}

def upsert_city(payload: dict):
    try:
        r = requests.post(
            f"{BACKEND}/config/cities",
            json=payload,
            headers={"X-User": "streamlit"},
            timeout=TIMEOUT_S,
        )
        r.raise_for_status()
        st.success("Ville enregistrée / mise à jour.")
        # Invalide le cache de get_config
        get_config.clear()
        st.rerun()
    except Exception as e:
        st.error(f"Echec POST /config/cities : {e}")

@st.cache_data(ttl=30)
def load_cities_active_options() -> list[tuple[str, str]]:
    cfg = get_config()
    cities = cfg.get("cities", [])
    return [(c["name"], c["id"]) for c in cities if c.get("active", True)]

def submit_query(payload: dict) -> dict | None:
    try:
        r = requests.post(f"{BACKEND}/query", json=payload, timeout=TIMEOUT_S + 7)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Echec POST /query : {e}")
        return None

def get_query_status(request_id: str) -> dict | None:
    try:
        r = requests.get(f"{BACKEND}/query/{request_id}", timeout=TIMEOUT_S)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Echec GET /query/{request_id} : {e}")
        return None

# ----------------------------
# Sidebar
# ----------------------------
with st.sidebar:
    st.subheader("Etat backend")
    try:
        pong = requests.get(f"{BACKEND}/", timeout=TIMEOUT_S).json()
        st.write("Backend OK", pong)
    except Exception as e:
        st.write("Backend KO", str(e))

# ----------------------------
# Tabs
# ----------------------------
tab1, tab2, tab3 = st.tabs(["Cities", "Queries", "Results"])

# ----------------------------
# Tab 1 — Cities (config)
# ----------------------------
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

        default_id = slugify(f"{name}-{country}".lower())
        city_id = st.text_input("ID (unique)", value=default_id, help="Ex: paris-fr")

        submit_city = st.form_submit_button("Enregistrer / Mettre à jour")
        if submit_city:
            payload = {
                "id": city_id.strip(),
                "name": name.strip(),
                "country": country.strip().upper(),
                "lat": float(lat),
                "lon": float(lon),
                "active": bool(active),
            }
            upsert_city(payload)

    st.markdown("---")
    st.subheader("Villes configurées")

    cities = cfg.get("cities", [])
    if cities:
        cities_sorted = sorted(
            cities,
            key=lambda x: (not x.get("active", True), x.get("name", "").lower())
        )
        st.dataframe(cities_sorted, use_container_width=True, hide_index=True)
    else:
        st.info("Aucune ville pour l’instant.")

# ----------------------------
# Tab 2 — Queries (soumission)
# ----------------------------
with tab2:
    st.subheader("Submit a query")

    city_options = load_cities_active_options()  # [(name, id), ...]
    ids_only = [cid for _, cid in city_options]

    def _format_city(cid: str) -> str:
        for name, i in city_options:
            if i == cid:
                return name
        return cid

    selected_cities = st.multiselect(
        "Cities",
        options=ids_only,
        format_func=_format_city,
        help="Sélectionne une ou plusieurs villes actives."
    )

    today = date.today()
    date_from = st.date_input("From", value=today)
    date_to = st.date_input("To", value=today)
    metrics = st.multiselect(
        "Metrics",
        ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"],
        default=["temperature_2m"]
    )
    agg = st.selectbox("Aggregation", ["avg", "min", "max", "none"], index=0)

    if st.button("Submit query"):
        if not selected_cities:
            st.warning("Sélectionne au moins une ville.")
        else:
            payload = {
                "city_ids": selected_cities,
                "date_from": date_from.isoformat() if date_from else None,
                "date_to": date_to.isoformat() if date_to else None,
                "metrics": metrics,
                "agg": None if agg == "none" else agg,
            }
            resp = submit_query(payload)
            if resp:
                request_id = resp.get("request_id")
                st.success(f"Query accepted. request_id={request_id}")
                st.code(json.dumps(resp, indent=2))
                # Mémorise pour l’onglet Results
                st.session_state["last_request_id"] = request_id

# ----------------------------
# Tab 3 — Results (statut)
# ----------------------------
with tab3:
    st.subheader("Check query status")

    default_req = st.session_state.get("last_request_id", "")
    req_id = st.text_input("request_id", value=default_req)
    if st.button("Check status") and req_id:
        resp = get_query_status(req_id)
        if resp is not None:
            st.json(resp)
