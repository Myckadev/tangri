# streamlit/app.py
import os
import io
import json
from datetime import date
import requests
import pandas as pd
import streamlit as st

# ---------- Config ----------
BACKEND = os.getenv("BACKEND_URL", "http://backend:8000")
PRODUCER_HTTP = os.getenv("PRODUCER_URL", "http://producer:8000")
WEBHDFS = os.getenv("WEBHDFS_URL", "http://namenode:9870/webhdfs/v1")
TIMEOUT_S = 10

# ---------- Utils ----------
try:
    from slugify import slugify  # optional dep
except Exception:
    def slugify(s: str) -> str:
        return (
            s.lower()
            .replace(" ", "-")
            .replace("_", "-")
            .replace("/", "-")
            .strip("-")
        )

st.set_page_config(page_title="OpenMeteo Control", layout="wide")
st.title("OpenMeteo — Pilotage & Requêtes")

# ---------- Backend helpers ----------
def backend_get(path: str):
    try:
        r = requests.get(f"{BACKEND}{path}", timeout=TIMEOUT_S)
        r.raise_for_status()
        # 304 handled upstream; here we assume 200
        return r.json()
    except Exception as e:
        st.error(f"GET {path} failed: {e}")
        return None

def backend_post(path: str, payload: dict):
    try:
        r = requests.post(f"{BACKEND}{path}", json=payload, timeout=TIMEOUT_S + 10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"POST {path} failed: {e}")
        return None

def backend_put(path: str, payload: dict):
    try:
        r = requests.put(f"{BACKEND}{path}", json=payload, timeout=TIMEOUT_S + 5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"PUT {path} failed: {e}")
        return None

def backend_delete(path: str):
    try:
        r = requests.delete(f"{BACKEND}{path}", timeout=TIMEOUT_S + 5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"DELETE {path} failed: {e}")
        return None

# ---------- HDFS helpers (WebHDFS) ----------
def hdfs_listdir(path: str):
    # path like /datalake/gold/queries/<request_id>
    params = {"op": "LISTSTATUS"}
    r = requests.get(f"{WEBHDFS}{path}", params=params, timeout=15, allow_redirects=True)
    r.raise_for_status()
    return r.json()["FileStatuses"]["FileStatus"]

def hdfs_read_file(path: str) -> bytes:
    # two-step open (redirect)
    params = {"op": "OPEN"}
    r1 = requests.get(f"{WEBHDFS}{path}", params=params, timeout=15, allow_redirects=False)
    if r1.status_code not in (200, 307):
        r1.raise_for_status()
    if r1.status_code == 200:
        return r1.content
    url2 = r1.headers["Location"]
    r2 = requests.get(url2, timeout=30)
    r2.raise_for_status()
    return r2.content

def load_parquet_from_hdfs_dir(dir_path: str) -> pd.DataFrame:
    files = hdfs_listdir(dir_path)
    parquet_files = [f for f in files if f["pathSuffix"].endswith(".parquet")]
    if not parquet_files:
        st.warning("Aucun fichier Parquet trouvé pour cette requête.")
        return pd.DataFrame()
    fname = parquet_files[0]["pathSuffix"]
    full = f"{dir_path}/{fname}"
    data = hdfs_read_file(full)
    bio = io.BytesIO(data)
    return pd.read_parquet(bio)  # requires pyarrow in image

# ---------- Tabs ----------
tab1, tab2, tab3 = st.tabs(["💠 Villes", "📈 Requêtes batch", "⚙️ Contrôles"])

# ===========================================
# Tab 1 — Villes (CRUD + géocodage)
# ===========================================
with tab1:
    st.subheader("Gestion des villes")

    # Get cities
    cities_doc = backend_get("/config/cities") or {}
    cities = cities_doc.get("cities", [])

    # Prefill session defaults (for geocoding)
    st.session_state.setdefault("prefill_name", "")
    st.session_state.setdefault("prefill_country", "FR")
    st.session_state.setdefault("prefill_lat", 43.2965)
    st.session_state.setdefault("prefill_lon", 5.3698)

    with st.form("create_city"):
        col1, col2, col3 = st.columns(3)
        with col1:
            name = st.text_input("Nom", value=st.session_state.get("prefill_name",""), placeholder="Marseille")
            country = st.text_input("Pays (ISO2)", value=st.session_state.get("prefill_country","FR"), max_chars=2)
        with col2:
            lat = st.number_input("Latitude", min_value=-90.0, max_value=90.0, value=float(st.session_state.get("prefill_lat", 43.2965)))
            lon = st.number_input("Longitude", min_value=-180.0, max_value=180.0, value=float(st.session_state.get("prefill_lon", 5.3698)))
        with col3:
            active = st.checkbox("Active", value=True)
            st.caption("L'ID est généré automatiquement depuis Nom+Pays.")
        submitted = st.form_submit_button("➕ Ajouter / Mettre à jour")
        if submitted:
            cid = slugify(f"{name}-{country}")
            payload = {"id": cid, "name": name, "country": country.upper(), "lat": lat, "lon": lon, "active": active}
            res = backend_post("/config/cities", payload)
            if res:
                st.success(f"Ville '{cid}' enregistrée")
                # Reset prefill (optionnel)
                st.session_state["prefill_name"] = ""
                st.session_state["prefill_country"] = "FR"
                st.session_state["prefill_lat"] = 43.2965
                st.session_state["prefill_lon"] = 5.3698
                st.rerun()

    st.markdown("---")
    st.caption("Recherche de ville (Open-Meteo Geocoding)")
    q = st.text_input("Nom de ville à rechercher", placeholder="ex: Marseille")
    if st.button("🔎 Rechercher"):
        try:
            r = requests.get(
                "https://geocoding-api.open-meteo.com/v1/search",
                params={"name": q, "count": 5, "language": "fr", "format": "json"},
                timeout=10
            )
            r.raise_for_status()
            results = r.json().get("results", []) or []
            if not results:
                st.warning("Aucun résultat.")
            else:
                options = {
                    f"{it['name']} — {it.get('country_code','')} (lat={it['latitude']}, lon={it['longitude']})": it
                    for it in results
                }
                sel = st.selectbox("Résultats :", list(options.keys()))
                if sel:
                    it = options[sel]
                    st.session_state["prefill_name"] = it["name"]
                    st.session_state["prefill_country"] = it.get("country_code","FR")
                    st.session_state["prefill_lat"] = it["latitude"]
                    st.session_state["prefill_lon"] = it["longitude"]
                    st.info("Valeurs pré-remplies. Remontez au formulaire pour valider.")
        except Exception as e:
            st.error(f"Recherche impossible: {e}")

    st.markdown("---")
    if cities:
        st.write("Villes connues :")
        for c in cities:
            cols = st.columns([2,1,1,1,1,1])
            with cols[0]:
                st.write(f"**{c['id']}** — {c['name']} ({c['country']})")
            with cols[1]:
                st.write(f"lat: {float(c['lat']):.4f}")
            with cols[2]:
                st.write(f"lon: {float(c['lon']):.4f}")
            with cols[3]:
                st.write("active ✅" if c.get("active", True) else "inactive ⛔")
            with cols[4]:
                if st.button("Basculer actif", key=f"toggle_{c['id']}"):
                    payload = {**c, "active": not c.get("active", True)}
                    res = backend_put(f"/config/cities/{c['id']}", payload)
                    if res:
                        st.success("Ville mise à jour")
                        st.rerun()
            with cols[5]:
                if st.button("🗑️", key=f"del_{c['id']}"):
                    res = backend_delete(f"/config/cities/{c['id']}")
                    if res:
                        st.success("Ville supprimée")
                        st.rerun()
    else:
        st.info("Aucune ville pour l’instant.")

# ===========================================
# Tab 2 — Requêtes batch (Kafka -> Airflow -> Spark -> HDFS -> lecture UI)
# ===========================================
with tab2:
    st.subheader("Requêtes batch")
    cities_doc = backend_get("/config/cities") or {}
    cities = cities_doc.get("cities", [])
    choices = [c["id"] for c in cities]

    city_ids = st.multiselect("Villes", choices, default=choices[:1])
    c1, c2, c3 = st.columns(3)
    with c1:
        date_from = st.date_input("Date from (UTC)", value=date.today())
    with c2:
        date_to = st.date_input("Date to (UTC)", value=date.today())
    with c3:
        metrics = st.multiselect("Metrics", ["temperature_2m","relative_humidity_2m","wind_speed_10m"], default=["temperature_2m"])

    agg = st.selectbox("Aggregation", ["avg","min","max"], index=0)

    if st.button("🚀 Lancer la requête"):
        payload = {
            "city_ids": city_ids,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "metrics": metrics,
            "agg": agg,
        }
        res = backend_post("/query", payload)
        if res and res.get("request_id"):
            st.session_state["last_request_id"] = res["request_id"]
            st.success(f"Requête soumise : {res['request_id']}")

    rid = st.session_state.get("last_request_id")
    if rid:
        st.markdown("---")
        st.write(f"**Dernière requête** : `{rid}`")
        colA, colB = st.columns([1,1])
        with colA:
            if st.button("🔄 Rafraîchir statut"):
                st.rerun()
        with colB:
            if st.button("🗑️ Oublier cette requête"):
                st.session_state.pop("last_request_id", None)
                st.rerun()

        stat = backend_get(f"/query/{rid}")
        if stat:
            st.code(json.dumps(stat, indent=2))
            if stat.get("status") == "done":
                path = stat.get("result_path")
                if path:
                    st.info(f"Lecture HDFS : {path}")
                    try:
                        # convert hdfs://namenode:8020 -> WebHDFS path
                        webhdfs_dir = path.replace("hdfs://namenode:8020", "")
                        df = load_parquet_from_hdfs_dir(webhdfs_dir)
                        if not df.empty:
                            st.dataframe(df)
                            # charts for agg columns
                            if "date" in df.columns:
                                num_cols = [c for c in df.columns if c.startswith(("avg_","min_","max_"))]
                                for col in num_cols:
                                    st.line_chart(df.set_index("date")[col], height=220)
                        else:
                            st.warning("Résultat vide pour cette requête.")
                    except Exception as e:
                        st.error(f"Lecture HDFS échouée: {e}")

# ===========================================
# Tab 3 — Contrôles Producer
# ===========================================
with tab3:
    st.subheader("Contrôles Producer")
    colA, colB, colC = st.columns(3)
    with colA:
        if st.button("⏸️ Pause"):
            try:
                requests.get(f"{PRODUCER_HTTP}/pause", timeout=5)
                st.success("Producer en pause")
            except Exception as e:
                st.error(e)
    with colB:
        if st.button("▶️ Resume"):
            try:
                requests.get(f"{PRODUCER_HTTP}/resume", timeout=5)
                st.success("Producer relancé")
            except Exception as e:
                st.error(e)
    with colC:
        ok = False
        try:
            r = requests.get(f"{PRODUCER_HTTP}/health", timeout=3)
            ok = r.status_code == 200
        except Exception:
            pass
        st.metric("Producer health", "OK" if ok else "DOWN")
