import streamlit as st
import pandas as pd
from database.cassandra_db import CassandraDB


db = CassandraDB()


def render(user_id):
    st.title(f"Your Recent Activity ({user_id})")

    try:
        df = db.get_interactions_by_user(user_id)

        if df.empty:
            st.info("No activity found for this user.")
            return

        st.dataframe(df)

        st.subheader("Daily Activity Summary")
        daily = db.get_daily_activity_count(user_id)
        st.bar_chart(daily.set_index("date")["count"])

        st.subheader("Inactivity Periods")
        inactive = db.compute_inactivity_periods(user_id)
        st.dataframe(inactive)

        st.subheader("Anomaly Detection (Spikes & Drops)")
        anomalies, spike, drop = db.detect_abnormal_activity(user_id)
        st.dataframe(anomalies)

        st.write(f"Spike threshold: {spike}")
        st.write(f"Drop threshold: {drop}")

    except Exception as e:
        st.error(f"Error in Activity Logs module: {e}")

