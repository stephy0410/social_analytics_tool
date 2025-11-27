import streamlit as st
import graphviz
import pandas as pd
from database import dgraph_db
from textblob import TextBlob
import altair as alt # Streamlit's built-in charting
import nltk
from nltk.corpus import stopwords
from collections import Counter
import re
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')
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
    
    influencers = dgraph_db.get_influencers(client, min_followers=3)
    
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
    
    st.subheader("🤖 AI Semantic Search (ChromaDB)")
    # Safely import your new file
    try:
        from database import chroma_db
    except ImportError:
        st.error("Missing database/chroma_db.py")
        st.stop()

    col_btn, col_search = st.columns([1, 3])

    with col_btn:
        # Button mimics loading "sentences.txt"
        if st.button("Load Posts to Chroma"):
            msg = chroma_db.load_from_csv('posts.csv')
            if "Error" in msg:
                st.error(msg)
            else:
                st.success(msg)

    with col_search:
        # Input mimics the "required_queries" list
        user_query = st.text_input("Enter a query (example: 'What habits support well-being?')")
        
        if user_query:
            st.markdown(f"**Query:** *'{user_query}'*")
            
            # Call the function that mimics collection.query()
            results = chroma_db.query_database(user_query)
            
            # Display results matching the class print loop format
            if results['documents']:
                for i, doc in enumerate(results['documents'][0]):
                    post_id = results['ids'][0][i]
                    # Display like: "1. Sentence text..."
                    st.info(f"**{i+1}.** {doc}")
                    st.caption(f"*(Source ID: {post_id})*")
            else:
                st.warning("No results found. Did you load the data?")
    st.markdown("---")
    st.subheader("🧠 Innovation: Sentiment Analytics")
    st.caption("Analyze the 'Vibe' of the search results (Positive vs. Negative).")

    topic_query = st.text_input("Analyze Sentiment for Topic:", key="sentiment_q")

    if st.button("Analyze Brand Health"):
        # 1. Search ChromaDB for relevant posts
        results = chroma_db.query_database(topic_query)
        if not results['documents']:
            st.warning("No data found.")
        else:
            # 2. Analyze Sentiment for each post
            sentiment_data = []
            documents = results['documents'][0]
            
            for doc in documents:
                analysis = TextBlob(doc)
                score = analysis.sentiment.polarity # -1 (Bad) to +1 (Good)
                
                # Categorize
                if score > 0.1: label = "Positive 🟢"
                elif score < -0.1: label = "Negative 🔴"
                else: label = "Neutral ⚪"
                
                sentiment_data.append({"Post": doc, "Score": score, "Sentiment": label})

            # 3. Visualize the Result
            df_sent = pd.DataFrame(sentiment_data)
            
            col_chart, col_raw = st.columns([2, 1])
            
            with col_chart:
                st.write("**Sentiment Distribution**")
                # Simple Bar Chart
                chart = alt.Chart(df_sent).mark_bar().encode(
                    x='Sentiment',
                    y='count()',
                    color='Sentiment'
                )
                st.altair_chart(chart, use_container_width=True)
                
                # Calculate Average
                avg_score = df_sent["Score"].mean()
                if avg_score > 0: st.success(f"Overall Vibe: Positive ({avg_score:.2f})")
                else: st.error(f"Overall Vibe: Negative ({avg_score:.2f})")

            with col_raw:
                st.write("**Key Posts**")
                for item in sentiment_data:
                    st.text(f"{item['Sentiment']}: {item['Post'][:50]}...")
    st.markdown("---")
    st.subheader("💡 Innovation: Topic Keyword Extractor")
    st.caption("Identifies the most frequent and relevant unique words in the search results.")
    
    topic_query_key = st.text_input("Find Key Topics in Posts about:", key="topic_key_q")

    if st.button("Extract Keywords"):

        try:
            stopwords.words('english')
        except:
            nltk.download('stopwords')

        # 1. Buscar posts relevantes en ChromaDB
        results = chroma_db.query_database(topic_query_key)
        
        if not results['documents']:
            st.warning("No se encontraron datos.")
        else:
            all_text = " ".join(results['documents'][0])
            
            # 2. Limpieza de Texto y Tokenización
            # Elimina puntuación y convierte a minúsculas
            words = re.findall(r'\b\w+\b', all_text.lower())
            
            # 3. Eliminar "Stop Words" (palabras comunes como 'the', 'a', 'is')
            stop_words = set(stopwords.words('english'))
            filtered_words = [word for word in words if word not in stop_words and len(word) > 2]
            
            # 4. Contar la Frecuencia
            word_counts = Counter(filtered_words)
            top_10 = word_counts.most_common(10)
            
            # 5. Visualizar los resultados
            st.write(f"**Top 10 Keywords for '{topic_query_key}':**")
            
            df_keywords = pd.DataFrame(top_10, columns=['Keyword', 'Frequency'])
            st.dataframe(df_keywords, hide_index=True)

            # Opcional: Gráfico de barras
            st.bar_chart(df_keywords.set_index('Keyword'))