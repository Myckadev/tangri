import streamlit as st
from datetime import datetime

st.set_page_config(page_title="OpenMeteo Streamlit Hello", layout="wide")
st.title("Hello from Streamlit")
st.write("This is the Frontend 2 (batch & config) — hello mode.")
st.info(f"Timestamp: {datetime.utcnow().isoformat()}")

with st.sidebar:
    st.success("hello sidebar")

tab1, tab2 = st.tabs(["Cities (hello)", "Queries (hello)"])
with tab1:
    st.write("Cities tab — hello")
    st.json({
        "version": 1,
        "cities": [{"id":"paris-fr","name":"Paris","country":"FR","lat":48.8566,"lon":2.3522,"active":True}]
    })
with tab2:
    st.write("Queries tab — hello")
    st.code("POST /query -> {'message':'hello query received', ...}")
