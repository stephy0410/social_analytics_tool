import streamlit as st
import graphviz
import pandas as pd
from database import dgraph_db

def render(current_user_id):
    st.markdown("## 🕸️ Network & AI Intelligence")

    #  1. CONNECT TO CLIENT -
    # We use the client object from dgraph.py
    stub = dgraph_db.create_client_stub()
    client = dgraph_db.create_client(stub)
    # 2. DATA LOADING CONTROLS
    with st.expander("⚙️ Admin: Data Loader"):
        st.caption("Click this only once to load your CSV data into Dgraph.")
        if st.button("Load CSV Data to Dgraph"):
            try:
                dgraph_db.create_schema(client)
                uids = dgraph_db.load_users_from_csv(client, 'users.csv') 
                dgraph_db.load_posts_from_csv(client, 'posts.csv')
                dgraph_db.create_follow_edges(client, 'follows.csv', uids)
                st.success("Data Loaded Successfully!")
            except Exception as e:
                st.error(f"Error loading data: {e}")

    #  3. VISUALIZATION 
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.subheader(f"Relationship Map: {current_user_id}")
        
        # A. Create the Graph Object
        g = graphviz.Digraph()
        g.attr(rankdir='LR') 
        g.attr('node', shape='circle', style='filled', color='lightblue')
        
        # B. Add Center Node (The User)
        g.node(current_user_id, color='gold', label=f"👤\n{current_user_id}")

        # C. Get 'Following' (Who I look at) 
        following_data = dgraph_db.get_following(client, current_user_id)
        if following_data:
            for user in following_data:
                g.edge(current_user_id, user['user_id'], color="#1f77b4", penwidth="2")

        # D. Get 'Followers' (Who looks at me) 
        followers_data = dgraph_db.get_followers(client, current_user_id)
        if followers_data:
            for user in followers_data:
                g.edge(user['user_id'], current_user_id, color="#ff7f0e", style="dashed")

        st.graphviz_chart(g)
        st.caption("🔵 Solid: You follow | 🟠 Dashed: Follows you")

    with col2:
        st.subheader("Metrics")
        # Mutuals function
        mutuals = dgraph_db.get_mutual_connections(client, current_user_id)
        st.metric("Mutual Friends", len(mutuals))
        
        # List the mutual names
        if mutuals:
            st.markdown("**Mutuals:**")
            for m in mutuals:
                st.text(f"- {m['user_id']}")

    st.markdown("---")

    # --- 4. INFLUENCER CHART ---
    st.subheader("🏆 Top Influencers")
    
    influencers = dgraph_db.get_influencers(client, min_followers=1)
    
    if influencers:
        # Transform the list of dicts into a DataFrame for the chart
        df = pd.DataFrame(influencers)
        
        if not df.empty:
            df = df.set_index('user_id')
            st.bar_chart(df['follower_count'])
        else:
            st.info("No influencers found.")
    else:
        st.info("No influencer data returned.")