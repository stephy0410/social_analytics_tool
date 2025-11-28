import streamlit as st
import pandas as pd
from database.mongodb import MongoDBManager
from streamlit import rerun

db = MongoDBManager()

def render(current_user_id):
    st.header("👤 User Profile (MongoDB)")

    # --- 1. Fetch profile ---
    profile = db.get_public_profile(current_user_id)

    if not profile:
        st.error("User not found in MongoDB")
        return

    # --- 2. Display profile info ---
    st.subheader("Basic Information")

    col1, col2 = st.columns(2)

    with col1:
        st.write(f"**Username:** {profile.get('username')}")
        st.write(f"**Full Name:** {profile.get('full_name')}")
        st.write(f"**Age:** {profile.get('age')}")

    with col2:
        st.write(f"**Gender:** {profile.get('gender')}")
        st.write(f"**Bio:** {profile.get('bio') or '*No bio set*'}")

    st.markdown("---")

    # --- 3. Edit profile ---
    st.subheader("✏️ Edit Profile")

    new_bio = st.text_area("Bio", profile.get("bio") or "")
    new_gender = st.selectbox(
        "Gender",
        ["unknown", "female", "male", "other"],
        index=["unknown", "female", "male", "other"].index(profile.get("gender"))
    )

    if st.button("Save Changes"):
        db.update_profile(current_user_id, {
            "bio": new_bio,
            "gender": new_gender
        })
        st.success("Profile updated!")
        rerun()

    st.markdown("---")

    # =====================================================================
    # 4. LINKED SOCIAL ACCOUNTS
    # =====================================================================
    st.subheader("🔗 Linked Social Accounts")

    linked = profile.get("linked_social_accounts", [])

    if linked:
        st.table(pd.DataFrame(linked))
    else:
        st.info("This user has no linked social accounts.")

    st.markdown("---")

    # ------------------------------
    # ADD NEW SOCIAL ACCOUNT
    # ------------------------------
    st.subheader("➕ Add New Social Account")

    platform = st.selectbox(
        "Platform",
        ["Instagram", "TikTok", "Twitter", "LinkedIn", "Facebook", "GitHub"]
    )

    handle = st.text_input("Handle", placeholder="@username")
    followers = st.number_input("Followers", min_value=0, step=100)
    posts = st.number_input("Posts", min_value=0, step=1)
    profile_url = st.text_input("Profile URL (optional)", placeholder="https://...")

    if st.button("Add Account"):
        if handle.strip() == "":
            st.error("Handle cannot be empty.")
        else:
            db.add_social_account(
                current_user_id,
                platform,
                handle,
                followers,
                posts,
                profile_url
            )
            st.success("Account added!")
            rerun()

    st.markdown("---")

    # ------------------------------
    # DELETE SOCIAL ACCOUNT
    # ------------------------------
    st.subheader("❌ Remove Linked Account")

    if linked:
        to_delete = st.selectbox(
            "Select account to remove",
            linked,
            format_func=lambda x: f"{x['platform_name']} — {x['handle']}"
        )

        if st.button("Delete Selected Account"):
            db.remove_social_account(
                current_user_id,
                to_delete["platform_name"],
                to_delete["handle"]
            )
            st.success("Account removed!")
            rerun()

    else:
        st.info("No accounts to remove.")

    st.markdown("---")

    # =====================================================================
    # 5. ANALYTICS
    # =====================================================================
    st.subheader("📊 Platform Popularity (MongoDB)")

    data = db.count_social_platform_usage()

    if data:
        df_analytics = pd.DataFrame(data).rename(columns={"_id": "platform"})
        df_analytics = df_analytics.set_index("platform")
        st.bar_chart(df_analytics["total_users"])
    else:
        st.info("No social platform data yet.")

    st.caption("MongoDB analytics and profile management.")

    # =======================================================================
    # 6. DELETE ACCOUNT (CONFIRMATION)
    # =======================================================================
    st.subheader("🗑️ Delete Account")

    st.warning("⚠️ This action is permanent. Your account and all related data will be deleted.")

    confirm = st.checkbox("I understand and want to delete my account permanently.")

    if confirm and st.button("Delete My Account"):
        try:
            db.delete_user(current_user_id)
            st.success("Your account has been deleted permanently.")

            # Destroy user session
            st.session_state.authenticated = False
            st.session_state.logged_user = None

            st.info("You will be redirected to the login page.")
            rerun()

        except Exception as e:
            st.error(f"Error deleting account: {e}")

# =======================================================================
    # 7. ACCOUNT STATUS CONTROL (TESTING RF8)
    # =======================================================================
    st.markdown("---")
    st.subheader("🛡️ Account Status Control (Prueba de RF8)")

    # Obtenemos el estado actual del usuario (asumiendo que profile es el doc de profiles)
    # Necesitamos el documento de users para el status
    user_doc = db.users.find_one({"username": current_user_id})
    current_status = user_doc.get('account_status', 'active')

    st.info(f"Estado Actual: **{current_status.upper()}**")

    col_status, col_btn = st.columns([0.6, 0.4])

    with col_status:
        # Nota: 'deleted' no se incluye aquí para forzar el uso de delete_user para eliminación permanente
        new_status = st.selectbox(
            "Seleccionar Nuevo Estado:",
            ["active", "suspended"],
            index=["active", "suspended"].index(current_status if current_status in ["active", "suspended"] else "active"),
            key="status_select"
        )

    with col_btn:
        st.markdown("<br>", unsafe_allow_html=True) # Espacio para alinear el botón
        if st.button(f"Aplicar Estado: {new_status.upper()}"):
            try:
                db.set_account_status(current_user_id, new_status)
                st.success(f"✅ Estado de cuenta actualizado a '{new_status.upper()}'.")
                rerun()

            except Exception as e:
                st.error(f"❌ Error al actualizar estado: {e}")

    # Este código de control debe ir antes del bloque de DELETE ACCOUNT (RF7)
    # que ya tienes en tu user_profile.py.