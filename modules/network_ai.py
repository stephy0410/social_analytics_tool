import streamlit as st
import graphviz
import pandas as pd
from database import dgraph_db
import altair as alt
import nltk
from nltk.corpus import stopwords
from collections import Counter
import re
import requests
import time

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')
def analyze_sentiment_hf_api(text, api_key, max_retries=3):
    API_URL = "https://router.huggingface.co/hf-inference/models/cardiffnlp/twitter-roberta-base-sentiment-latest"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "x-wait-for-model": "true"
    }

    for attempt in range(max_retries):
        try:
            payload = {"inputs": text}
            response = requests.post(API_URL, headers=headers, json=payload, timeout=90)

            if response.status_code == 200:
                result = response.json()
                
                # 1. Parse the nested list structure
                # API returns: [[{'label': 'LABEL_0', 'score': 0.9}, {'label': 'LABEL_1', 'score': 0.1}, ...]]
                if isinstance(result, list) and len(result) > 0:
                    first_input_scores = result[0]
                    
                    # Find the label with the highest score
                    if isinstance(first_input_scores, list):
                        top_result = max(first_input_scores, key=lambda x: x['score'])
                    else:
                        # Fallback if API returns flat dict
                        top_result = first_input_scores

                    # 2. Map roberta labels to your UI labels
                    # LABEL_0: Negative, LABEL_1: Neutral, LABEL_2: Positive
                    roberta_map = {
                        "LABEL_0": "Negative 🔴",
                        "LABEL_1": "Neutral ⚪",
                        "LABEL_2": "Positive 🟢"
                    }
                    
                    api_label = top_result.get('label', '')
                    # If the API returns NEGATIVE/POSITIVE (legacy), use it, otherwise map LABEL_x
                    final_label = roberta_map.get(api_label, api_label)
                    
                    # Normalize legacy caps if needed
                    if final_label == "NEGATIVE": final_label = "Negative 🔴"
                    if final_label == "POSITIVE": final_label = "Positive 🟢"
                    if final_label == "NEUTRAL": final_label = "Neutral ⚪"

                    return {'label': final_label, 'score': top_result.get('score', 0.0)}
                
                return None

            elif response.status_code == 503:
                wait_time = 20 * (attempt + 1)  # 20s, 40s, 60s
                st.warning(f"⏳ Model loading... Waiting {wait_time}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                st.error(f"API Error {response.status_code}: {response.text}")
                return None

        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                st.warning(f"⏱️ Timeout. Retrying... (attempt {attempt + 1}/{max_retries})")
                time.sleep(10 * (attempt + 1))
                continue
            else:
                st.error("⏱️ Request timed out after multiple attempts.")
                return None
        except Exception as e:
            st.error(f"Request failed: {str(e)}")
            return None
    
    return None
    
def render(current_user_id):
    st.markdown("## 🕸️ Network & Engagement Analytics")
    
    stub = dgraph_db.create_client_stub()
    client = dgraph_db.create_client(stub)

    # --- 1. ADMIN & REAL-TIME ---
    with st.expander("⚙️ Admin & Real-Time Actions", expanded=False):
        tab1, tab2, tab3 = st.tabs(["Data Load", "Follow User", "Interact with Post"])
        
        with tab1:
            st.write("**Initialize Database with Sample Data**")
            col1, col2 = st.columns(2)
            
            if col1.button("🔥 Load All CSV Data"):
                try:
                    with st.spinner("Loading data..."):
                        dgraph_db.create_schema(client)
                        dgraph_db.load_users_from_csv(client, 'users.csv')
                        dgraph_db.load_posts_from_csv(client, 'posts.csv')
                        dgraph_db.load_interactions_from_csv(client, 'interactions.csv')
                        dgraph_db.create_follow_edges(client, 'follows.csv')
                    st.success("✅ Database initialized with sample data!")
                except Exception as e:
                    st.error(f"❌ Error loading data: {e}")
            
            if "show_reset_confirm" not in st.session_state:
                st.session_state.show_reset_confirm = False
            
            if col2.button("⚠️ RESET DB", key="reset_btn"):
                st.session_state.show_reset_confirm = True
            
            if st.session_state.show_reset_confirm:
                confirm = st.checkbox("✅ I understand this will delete all data permanently")
                if confirm:
                    if st.button("🗑️ Confirm Reset"):
                        with st.spinner("Dropping all data..."):
                            success = dgraph_db.drop_all_data(client)
                            if success:
                                st.success("✅ Database has been reset successfully!")
                                st.rerun()
                            else:
                                st.error("❌ Failed to reset database.")
        
        with tab2:
            st.write("**Follow Another User**")
            all_users = dgraph_db.get_all_users(client)
            user_options = [u['user_id'] for u in all_users if u['user_id'] != current_user_id]
            
            if user_options:
                target_user = st.selectbox("Select user to follow:", user_options)
                if st.button("👥 Follow User"):
                    ok, msg = dgraph_db.add_realtime_follow(client, current_user_id, target_user)
                    if ok: st.success(f"✅ {msg}"); st.rerun()
                    else: st.error(f"❌ {msg}")
            else:
                st.info("No other users found. Load sample data first.")
        
        with tab3:
            st.write("**Interact with Posts**")
            post_id = st.text_input("Post ID (e.g., post_101)", placeholder="Enter post ID...")
            action = st.selectbox("Action Type", ["Like", "Comment", "Share"])
            
            if st.button(f"🎯 Submit {action}"):
                if not post_id:
                    st.error("❌ Please enter a Post ID")
                else:
                    with st.spinner("Processing interaction..."):
                        ok, msg = dgraph_db.add_realtime_interaction(client, current_user_id, post_id, action)
                    if ok: st.success(f"✅ {msg}"); st.rerun()
                    else: st.error(f"❌ {msg}")

    # --- 2. ENGAGEMENT METRICS ---
    st.subheader("📊 User Engagement Metrics")
    with st.spinner("Loading engagement data..."):
        metrics = dgraph_db.get_engagement_metrics(client, current_user_id)
    
    if metrics and metrics.get('post_count', 0) > 0:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Posts", metrics['post_count'])
        col2.metric("Total Likes", metrics['total_likes'])
        col3.metric("Total Comments", metrics['total_comments'])
        col4.metric("Total Shares", metrics['total_shares'])
        
        st.write("**📈 Engagement per Post**")
        posts_df = pd.DataFrame(metrics['posts'])
        
        if not posts_df.empty:
            posts_df['score'] = posts_df['likes'] + posts_df['comments'] * 2 + posts_df['shares'] * 3
            st.dataframe(posts_df.sort_values('score', ascending=False), use_container_width=True)
            
            st.write("**📊 Engagement Distribution**")
            chart_data = posts_df[['post_id', 'likes', 'comments', 'shares']].set_index('post_id')
            st.bar_chart(chart_data.head(10))
        else:
            st.info("No post-level engagement data available.")

        st.markdown("---")

        st.write("**📈 Follower Growth Over Time**")
        growth_data = dgraph_db.get_follower_growth(client, current_user_id)

        if growth_data:
            timestamps = []
            for f in growth_data:
                ts = f.get('timestamp') 
                if ts:
                    timestamps.append(ts)
            
            if timestamps:
                df_growth = pd.DataFrame({'timestamp': pd.to_datetime(timestamps)})
                df_growth['Date'] = df_growth['timestamp'].dt.date
                daily_counts = df_growth.groupby('Date').size().reset_index(name='New Followers')
                daily_counts = daily_counts.sort_values('Date')
                
                daily_counts['Total Followers'] = daily_counts['New Followers'].cumsum()
                
                cutoff_date = pd.Timestamp.now().date() - pd.Timedelta(days=365)
                daily_counts = daily_counts[daily_counts['Date'] >= cutoff_date]
                
                chart = alt.Chart(daily_counts).mark_line(point=True).encode(
                    x=alt.X('Date:T', title='Date'),
                    y=alt.Y('Total Followers:Q', title='Total Followers'),
                    tooltip=['Date', 'New Followers', 'Total Followers']
                ).properties(height=300)
                
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("Follower data exists, but no timestamps were found (legacy data).")
                st.caption("💡 Tip: New follow relationships will include timestamps. Try following some users!")
        else:
            st.info("No followers found yet.")
    else:
        st.info("💡 No posts or engagement data found for this user. Try:")
        st.write("1. Load sample data using the 'Data Load' tab above")
        st.write("2. Create posts with your user as the author")
        st.write("3. Ask other users to interact with your posts")

    st.markdown("---")

    # --- 3. GRAPH VISUALIZATION & COMMUNITIES ---
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.subheader(f"Relationship Map: {current_user_id}")
        
        g = graphviz.Digraph()
        g.attr(rankdir='LR') 
        g.attr('node', shape='circle', style='filled', color='lightblue')
        
        g.node(current_user_id, color='gold', label=f"👤\n{current_user_id}")

        following_data = dgraph_db.get_following(client, current_user_id)
        visible_nodes = {u['user_id'] for u in following_data}
        visible_nodes.add(current_user_id)

        if following_data:
            for user in following_data:
                uid = user['user_id']
                strength = user.get('relationship_strength', 0.1)
                width = str(1 + strength * 4)
                g.edge(current_user_id, uid, color="#1f77b4", penwidth=width)
                
                if 'secondary' in user:
                    for friend_of_friend in user['secondary']:
                        target_id = friend_of_friend['user_id']
                        if target_id in visible_nodes and target_id != current_user_id:
                            g.edge(uid, target_id, color="#555555", style="dotted", constraint="false")

        followers_data = dgraph_db.get_followers(client, current_user_id)
        if followers_data:
            for user in followers_data:
                g.edge(user['user_id'], current_user_id, color="#ff7f0e", style="dashed")

        st.graphviz_chart(g)
        st.caption("🔵 Solid: You follow | 🟠 Dashed: Follows you | ⚫ Dotted: Friends follow each other")

    with col2:
        st.subheader("Network Insights")
        mutuals = dgraph_db.get_mutual_connections(client, current_user_id)
        st.metric("Mutual Friends", len(mutuals))
        if mutuals:
            with st.expander("View Mutuals"):
                for m in mutuals:
                    st.text(f"• {m['user_id']}")

        st.markdown("---")
        
        st.write("**🏰 Communities**")
        st.caption("Triangles (You ↔ A ↔ B)")
        clusters = dgraph_db.get_community_clusters(client, current_user_id)
        
        if clusters:
            for i, cluster in enumerate(clusters):
                names = ", ".join(cluster)
                st.success(f"**Cluster {i+1}:**\n{names}")
        else:
            st.info("No clusters detected.")

        st.markdown("---")

    # --- 4. PROPAGATION ANALYSIS ---
    st.subheader("🔗 Information Propagation Path")
    col_p1, col_p2, col_p3 = st.columns([1, 1, 1])
    
    with col_p1:
        start_user = st.text_input("Source User", value=current_user_id)
    with col_p2:
        end_user = st.text_input("Target User", placeholder="Enter username...")
    with col_p3:
        st.write("")
        if st.button("🔍 Trace Path"):
            if not end_user:
                st.warning("Please enter a target user")
            else:
                with st.spinner("Finding shortest path..."):
                    path = dgraph_db.find_shortest_path(client, start_user, end_user)
                if path:
                    chain = " ➡️ ".join([n['user_id'] for n in path])
                    st.success(f"**Path Found:** {chain}")
                else:
                    st.warning("❌ No connection path found")

    # --- 5. INFLUENCERS ---
    st.subheader("🏆 Top Influencers")
    influencers = dgraph_db.get_influencers(client, min_followers=3)
    if influencers:
        df = pd.DataFrame(influencers)
        if not df.empty:
            df = df.set_index('user_id')
            st.bar_chart(df['follower_count'])
        else: st.info("No influencers found.")
    else: st.info("No influencer data returned.")
    
    st.markdown("---")
    
    # --- 6. SEMANTIC SEARCH ---
    st.subheader("🤖 AI Semantic Search (ChromaDB)")
    try:
        from database import chroma_db
    except ImportError:
        st.error("Missing database/chroma_db.py")
        st.stop()

    col_btn, col_search = st.columns([1, 3])
    with col_btn:
        if st.button("Load Posts to Chroma"):
            msg = chroma_db.load_from_csv('posts.csv')
            if "Error" in msg: st.error(msg)
            else: st.success(msg)

    with col_search:
        user_query = st.text_input("Enter a query (example: 'What habits support well-being?')")
        if user_query:
            st.markdown(f"**Query:** *'{user_query}'*")
            results = chroma_db.query_database(user_query)
            if results['documents']:
                for i, doc in enumerate(results['documents'][0]):
                    post_id = results['ids'][0][i]
                    st.info(f"**{i+1}.** {doc}")
                    st.caption(f"*(Source ID: {post_id})*")
            else: st.warning("No results found.")
    
    st.markdown("---")
    
    # 7.  SENTIMENT ANALYSIS  (compact by default)
    st.subheader("🧠 Innovation: Sentiment Analytics")
    #hide insstructions
    show_full_hf_ui = st.checkbox("Show HF details", value=False)

    if show_full_hf_ui:                       
        try:
            default_key = st.secrets.get("HF_API_KEY", "")
        except Exception:
            default_key = ""

        hf_api_key = st.text_input(
            "🔑 Hugging Face API Key",
            value=default_key,
            type="password",
            help="Get your free API key from https://huggingface.co/settings/tokens"
        )

        if default_key:
            st.success("✅ API key loaded from secrets file")
        else:
            st.caption("📝 [Get your free API key here](https://huggingface.co/settings/tokens)")
            st.info("💡 Tip: Save your key in `.streamlit/secrets.toml` to avoid pasting it each time")
    else:
        
        try:
            hf_api_key = st.secrets.get("HF_API_KEY", "")
        except Exception:
            hf_api_key = ""

    # always visible
    topic_query = st.text_input("Analyze Sentiment for Topic:", key="sentiment_q")

    if st.button("Analyze Brand Health"):
        if not hf_api_key:
            st.warning("⚠️  Please enter your Hugging Face API key (tick ‘Show HF details’) or store it in secrets.toml")
            st.stop()

        results = chroma_db.query_database(topic_query)
        if not results['documents']:
            st.warning("No data found.")
        else:
            sentiment_data, total = [], len(results['documents'][0])
            progress = st.progress(0)

            for idx, doc in enumerate(results['documents'][0]):
                progress.progress((idx + 1) / total)
                out = analyze_sentiment_hf_api(doc, hf_api_key)
                if out:
                    sentiment_data.append({
                        "Post": doc,
                        "Score": round(out['score'], 2),
                        "Sentiment": out['label']
                    })
                else:
                    st.warning(f"⚠️  Failed to analyse post {idx + 1}")

            progress.empty()

            if not sentiment_data:
                st.error("❌ Could not analyze any posts – check key / model status")
                st.stop()

            st.info(f"📊  Analyzed {len(sentiment_data)} posts")

            df_sent = pd.DataFrame(sentiment_data)
            col_chart, col_raw = st.columns([2, 1])

            with col_chart:
                st.write("**Sentiment Distribution**")
                chart = (
                    alt.Chart(df_sent)
                    .mark_bar()
                    .encode(
                        x=alt.X("Sentiment", title="Sentiment Class"),
                        y=alt.Y("count()", title="Number of Posts"),
                        color=alt.Color(
                            "Sentiment",
                            scale=alt.Scale(
                                domain=["Positive 🟢", "Neutral ⚪", "Negative 🔴"],
                                range=["#2ecc71", "#95a5a6", "#e74c3c"],
                            ),
                        ),
                        tooltip=["Sentiment", "count()", "mean(Score)"],
                    )
                    .interactive()
                )
                st.altair_chart(chart, use_container_width=True)

            with col_raw:
                st.write("**Key Posts**")
                for it in sentiment_data:
                    st.text(f"{it['Sentiment']} ({it['Score']}):\n{it['Post'][:60]}...")

    st.markdown("---")
    # --- 8. KEYWORD EXTRACTION ---
    st.subheader("💡 Innovation: Topic Keyword Extractor")
    topic_query_key = st.text_input("Find Key Topics in Posts about:", key="topic_key_q")
    if st.button("Extract Keywords"):
        results = chroma_db.query_database(topic_query_key)
        if not results['documents']:
            st.warning("No data found.")
        else:
            all_text = " ".join(results['documents'][0])
            words = re.findall(r'\b\w+\b', all_text.lower())
            stop_words = set(stopwords.words('english'))
            filtered_words = [word for word in words if word not in stop_words and len(word) > 2]
            word_counts = Counter(filtered_words)
            df_keywords = pd.DataFrame(word_counts.most_common(10), columns=['Keyword', 'Frequency'])
            st.bar_chart(df_keywords.set_index('Keyword'))