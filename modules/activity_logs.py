import streamlit as st
import pandas as pd
from database.cassandra_db import CassandraDB

db = CassandraDB()


def render(current_user_id: str):
    st.header("📘 Activity Logs – Cassandra")

    # --- Botón para cargar CSV real ---
    st.subheader("Load Activity Logs from CSV")

    if st.button("📥 Import activity_logs.csv"):
        try:
            db.load_demo_from_csv("activity_logs.csv")
            st.success("CSV imported into Cassandra successfully.")
        except Exception as e:
            st.error(f"Error importing CSV: {e}")

    st.markdown("---")

    # --- Mostrar actividad del usuario actual ---
    st.subheader(f"Your Recent Activity ({current_user_id})")

    try:
        logs = db.get_activity_by_user(current_user_id)

        if not logs:
            st.info("No activity found yet for this user.")
            return

        df = pd.DataFrame(logs).sort_values("timestamp", ascending=False)
        st.dataframe(df)

        # -------- Filtro por tipo de acción ----------
        st.subheader("Filter by action type")

        available_actions = sorted({row["action"] for row in logs})
        selected_action = st.selectbox("Select action", available_actions)

        filtered = db.get_activities_by_type(current_user_id, selected_action)
        st.write(f"Found {len(filtered)} activities for '{selected_action}'")

        st.dataframe(pd.DataFrame(filtered))

    except Exception as e:
        st.error(f"Error in Activity Logs module: {e}")

    if st.button("🗑️ Clear All Activity Logs (truncate table)"):
        try:
            db.session.execute("TRUNCATE activity_logs")
            st.success("All activity logs deleted.")
        except Exception as e:
            st.error(f"Error clearing logs: {e}")
