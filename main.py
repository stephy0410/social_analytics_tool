import streamlit as st
import sys
import os

# --- FIX: Add only the project root to sys.path ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

sys.path.insert(0, os.path.join(PROJECT_ROOT, "modules"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "database"))


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
    from modules import user_profile
    modules_status["profile"] = True
except Exception as e:
    st.sidebar.error(f"MongoDB Module Error: {e}")

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

# --- LOGIN SYSTEM (MongoDB + bcrypt) ---
import bcrypt
from database.mongodb import MongoDBManager

db_auth = MongoDBManager()
try:
    # Check if the 'users' collection is empty
    if db_auth.users.count_documents({}) == 0:
        print("⚠️ Database is empty! Auto-loading users from CSV...")
        
        # Call the function to load users (Ensure this exists in your mongodb.py)
        db_auth.load_users_from_csv("users.csv") 
        
        print("✅ Auto-load complete.")
except Exception as e:
    print(f"Auto-load warning: {e}")
# Initialize session state
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "logged_user" not in st.session_state:
    st.session_state.logged_user = None

def login_screen():
    st.title("🔐 Login to Social Analytics Tool")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        user = db_auth.users.find_one({"username": username})

        if user:

            # ===============================
            #  🚫 Account status validation
            # ===============================
            status = user.get("account_status", "active")
            if status != "active":
                st.error(f"Your account is **{status}**. Login is not allowed.")
                st.stop()

            # ===============================
            #  🔐 Password validation
            # ===============================
            stored_hash = user.get("password_hash").encode()

            if bcrypt.checkpw(password.encode(), stored_hash):
                st.session_state.authenticated = True
                st.session_state.logged_user = username
                st.success("Welcome!")
                st.rerun()
            else:
                st.error("Incorrect password.")

        else:
            st.error("Username not found.")

    # NEW BUTTON
    if st.button("Create new account"):
        st.session_state.show_signup = True
        st.rerun()

    st.stop()


def signup_screen():
    st.title("📝 Create Account")

    full_name = st.text_input("Full Name")
    username = st.text_input("Username")
    email = st.text_input("Email")
    password = st.text_input("Password", type="password")
    age = st.number_input("Age", min_value=1, max_value=120)
    gender = st.selectbox("Gender", ["female", "male", "other", "unknown"])

    if st.button("Create Account"):
        try:
            user_id = db_auth.create_user(
                username=username,
                email=email,
                password=password,
                full_name=full_name,
                age=age,
                gender=gender,
            )

            st.success("Account created successfully! You can now log in.")
            st.session_state.show_signup = False
            st.rerun()

        except ValueError as e:
            st.error(str(e))


# If user clicked "Create account", show signup screen
if st.session_state.get("show_signup"):
    signup_screen()
    st.stop()

# If not authenticated → show login ONLY
if not st.session_state.authenticated:
    login_screen()



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