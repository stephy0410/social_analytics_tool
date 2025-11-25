import streamlit as st
import sys

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

try:
    from modules import user_profile
    modules_status["profile"] = True
except ImportError:
    pass  #Hannah hasn't finished their file yet

try:
    from modules import activity_trends
    modules_status["activity"] = True
except ImportError:
    pass  # Fernando hasn't finished their file yet

try:
    from modules import network_ai
    modules_status["network"] = True
except ImportError:
    pass  #  Stephy hasn't finished their file yet

# --- 2. SIDEBAR (Global User Selection) ---
st.sidebar.title("Analytics Controls")

# MOCK DATA: Since we aren't connecting to MongoDB yet, use this list.
dummy_users = [
    {"id": "user_001", "name": "Hannah Chenoa", "handle": "@hannah1"},
    {"id": "user_002", "name": "Luis Fernando", "handle": "@luis_fer"},
    {"id": "user_003", "name": "Stephanie B",   "handle": "@steph_b"}
]

# The Dropdown
selected_handle = st.sidebar.selectbox(
    "Select User to Analyze",
    options=[u["handle"] for u in dummy_users]
)

# Get the ID (This is the "Key" that passes to every module)
current_user_id = next(item["id"] for item in dummy_users if item["handle"] == selected_handle)
current_user_name = next(item["name"] for item in dummy_users if item["handle"] == selected_handle)

st.sidebar.markdown(f"**Active ID:** `{current_user_id}`")
st.sidebar.markdown("---")
st.sidebar.info("System Status: \n" + 
                f"- Mongo Module: {'✅' if modules_status['profile'] else '❌'}\n" +
                f"- Cassandra Module: {'✅' if modules_status['activity'] else '❌'}\n" +
                f"- Dgraph Module: {'✅' if modules_status['network'] else '❌'}")

# --- 3. MAIN CONTENT AREA ---
st.title(f"Dashboard: {current_user_name}")

# Create the Tabs
tab1, tab2, tab3 = st.tabs([
    "👤 User Profile (Mongo)", 
    "📈 Activity Logs (Cassandra)", 
    "🕸️ Network & AI (Dgraph/Chroma)"
])

# --- TAB 1: USER PROFILE ---
with tab1:
    if modules_status["profile"]:
        # We assume Hannah will make a function called 'render'
        try:
            user_profile.render(current_user_id)
        except Exception as e:
            st.error(f"Error loading Teammate A's module: {e}")
    else:
        st.warning("⚠️ user_profile.py not found. (Waiting for Teammate A)")

# --- TAB 2: ACTIVITY TRENDS ---
with tab2:
    if modules_status["activity"]:
        # We assume Fernando will make a function called 'render'
        try:
            activity_trends.render(current_user_id)
        except Exception as e:
            st.error(f"Error loading Teammate B's module: {e}")
    else:
        st.warning("⚠️ activity_trends.py not found. (Waiting for Teammate B)")

# --- TAB 3: NETWORK & AI (YOUR PART) ---
with tab3:
    if modules_status["network"]:
        try:
            network_ai.render(current_user_id)
        except Exception as e:
            st.error(f"Error loading your module: {e}")
    else:
        st.info("🛠️ You haven't created 'modules/network_ai.py' yet.")