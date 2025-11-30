import streamlit as st
import pandas as pd
from database.cassandra_db import CassandraDB

db = CassandraDB()

def render(current_user_id: str):
    st.header("📘 User Interactions – Cassandra")

    # --- Cargar interactions.csv ---
    st.subheader("Load interactions.csv into Cassandra")

    if st.button("📥 Import interactions.csv"):
        try:
            db.load_interactions_from_csv("interactions.csv")
            st.success("interactions.csv imported into Cassandra successfully.")
        except Exception as e:
            st.error(f"Error importing CSV: {e}")

    st.markdown("---")

    # --- Mostrar actividad del usuario actual ---
    st.subheader(f"Recent Interactions for {current_user_id}")

    try:
        logs = db.get_interactions_by_user(current_user_id)

        if not logs:
            st.info("No interactions found for this user yet.")
            return

        df = pd.DataFrame(logs).sort_values("timestamp", ascending=False)
        st.dataframe(df)

        # -------- Filtro por tipo de interacción ----------
        st.subheader("Filter by interaction type")

        available_types = sorted({row["interaction_type"] for row in logs})
        selected_type = st.selectbox("Select interaction type", available_types)

        filtered = db.get_interactions_by_type(current_user_id, selected_type)
        st.write(f"Found {len(filtered)} interactions of type '{selected_type}'")

        st.dataframe(pd.DataFrame(filtered))

    except Exception as e:
        st.error(f"Error in Interactions module: {e}")

    if st.button("🗑️ Clear All Interactions (truncate table)"):
        try:
            db.session.execute("TRUNCATE interactions")
            st.success("All interactions deleted.")
        except Exception as e:
            st.error(f"Error clearing interactions: {e}")
