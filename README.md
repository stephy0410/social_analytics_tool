# Social Media Analytics Tool 📊
 
**Authors:** Stephanie Borrego Arroyo, Hannah Chenoa Puente Rosales, Luis Fernando Del Real Vázquez  


## 📖 Project Overview

The **Social Media Analytics Tool** is a comprehensive dashboard designed to monitor user activity, analyze social networks, and track engagement trends. 

This project demonstrates a **Polyglot Persistence** architecture, utilizing the strengths of four distinct NoSQL databases to handle specific data requirements:
1.  **MongoDB** (Document): User profiles and authentication.
2.  **Cassandra** (Wide-Column): High-volume activity logging (Time-series).
3.  **Dgraph** (Graph): Complex relationship tracking and influence analysis.
4.  **ChromaDB** (Vector): AI-powered semantic search and sentiment analysis.

## 🏗️ System Architecture

The system is built using **Python** with a **Streamlit** frontend, orchestrating data flow between the following modules:

### 1. User Management (MongoDB)
* **Purpose:** Securely manages user identities and public profiles.
* **Key Features:** * Split-collection design (`users` for auth, `profiles` for public data) for security.
    * Dynamic aggregation of linked social accounts (Instagram, LinkedIn, X).
    * Authentication using `bcrypt` encryption.

### 2. Activity Logging (Cassandra)
* **Purpose:** Handles high-throughput write operations for user actions.
* **Key Features:**
    * Logs interactions (Likes, Comments, Shares) as time-series data.
    * Optimized for chronological queries (e.g., "Last 50 interactions").
    * Wide-column schema design for fast retrieval by User ID and Timestamp.

### 3. Network & Relationships (Dgraph)
* **Purpose:** Models the complex web of user connections.
* **Key Features:**
    * Tracks `FOLLOWS`, `LIKED_POST`, and `COMMENTED_POST` as graph edges.
    * Calculates "Relationship Strength" based on interaction frequency.
    * Performs advanced traversals: Mutual friends, community clusters (triangles), and influence propagation paths.

### 4. AI & Innovation (ChromaDB + Hugging Face)
* **Purpose:** Provides semantic understanding of content.
* **Key Features:**
    * **Semantic Search:** Finds posts conceptually similar to a query (not just keyword matching).
    * **Sentiment Analysis:** Analyzes post content to determine positive/negative sentiment.
    * **Topic Extraction:** Generates keyword corpora from post context.
    * Uses `all-MiniLM-L6-v2` for generating vector embeddings locally.

## 🚀 Installation & Setup

### Prerequisites
Ensure you have the following installed:
* Python 3.9+
* **Database Instances:** You must have running instances (local or dockerized) of:
    * MongoDB (Port 27017)
    * Cassandra (Port 9042)
    * Dgraph (Alpha: 9080, Zero: 5080)
    * ChromaDB (Embedded runs locally, no server needed)

### Dependencies
Install the required Python libraries:

```bash
pip install streamlit pymongo cassandra-driver pydgraph chromadb bcrypt pandas
