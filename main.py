import streamlit as st
import sys
import os

# --- FIX: Add only the project root to sys.path ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)
MODULES_DIR = "modules"
NETWORK_AI_FILE = os.path.join(MODULES_DIR, "network_ai.py")


# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Social Media Analytics Tool",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 1. SAFE IMPORT SYSTEM ---
modules_status = {
    "profile": False,
    "activity": False,
    "network": False
}

# Try flat imports first
try:
    import user_profile
    modules_status["profile"] = True
except:
    pass

try:
    import activity_trends
    modules_status["activity"] = True
except:
    pass

try:
    from modules import network_ai
    modules_status["network"] = True
except:
    pass


# --- 2. SIDEBAR (Dynamic User Selection) ---
st.sidebar.title("Analytics Controls")

real_users = []
try:
    if modules_status["network"]:
        from database import dgraph_db

        stub = dgraph_db.create_client_stub()
        client = dgraph_db.create_client(stub)

        # Get all Graph users
        real_users = dgraph_db.get_all_users(client)

except Exception as e:
    st.sidebar.error(f"Dgraph Connection Error: {e}")


# --- Dropdown logic ---
if real_users:
    user_list = [u['user_id'] for u in real_users]

    st.sidebar.success(f"Connected: {len(user_list)} users found.")

    selected_user_id = st.sidebar.selectbox(
        "Select User to Analyze",
        options=user_list
    )

    current_user_id = selected_user_id
    current_user_name = selected_user_id.replace("_", " ").title()

else:
    st.sidebar.warning("*No users found in Dgraph.*")
    st.sidebar.info("Go to Network & AI → Admin: Data Loader to upload your CSVs.")

    selected_user_id = "hannah_chenoa"
    current_user_id = "hannah_chenoa"
    current_user_name = "System Waiting..."


st.sidebar.markdown(f"**Active ID:** `{current_user_id}`")
st.sidebar.markdown("---")


# --- 3. MAIN CONTENT ---
st.title(f"Dashboard: {current_user_name}")

tab1, tab2, tab3 = st.tabs([
    "👤 User Profile (Mongo)",
    "📈 Activity Logs (Cassandra)",
    "🕸️ Network & AI (Dgraph/Chroma)"
])


# --- TAB 1: PROFILE ---
with tab1:
    if modules_status["profile"]:
        try:
            user_profile.render(current_user_id)
        except Exception as e:
            st.error(f"Error in Profile module: {e}")
    else:
        st.warning(" user_profile.py not found.")


# --- TAB 2: ACTIVITY ---
with tab2:
    if modules_status["activity"]:
        try:
            activity_trends.render(current_user_id)
        except Exception as e:
            st.error(f"Error in Activity module: {e}")
    else:
        st.warning(" activity_trends.py not found.")


# --- TAB 3: NETWORK (YOUR PART) ---
with tab3:
    if modules_status["network"]:
        try:
            network_ai.render(current_user_id)
        except Exception as e:
            st.error(f"Error in Network module: {e}")
    else:
        st.info(f"🛠️ File not found: {NETWORK_AI_FILE}")
