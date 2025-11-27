import chromadb
import csv
import os

client = chromadb.Client()

# Get or create the collection 
collection = client.get_or_create_collection(name="social_posts_project")

def load_from_csv(file_path='posts.csv'):
    documents = []
    ids = []
    
    # Check if file exists
    if not os.path.exists(file_path):
        return f"Error: '{file_path}' not found."

    # Read the CSV 
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            content = row.get('content', '').strip()
            if content:
                documents.append(content)
                ids.append(row['post_id'])

    if documents:
        try:
            collection.add(
                documents=documents,
                ids=ids
            )
            return f"Successfully loaded {len(documents)} posts from '{file_path}'."
        except Exception as e:
            return f"Error adding to Chroma: {e}"
    else:
        return "No content found in CSV."

def query_database(query_text):
    """
    Performs the query using class logic:
    collection.query(query_texts=[...], n_results=3) [cite: 46-49]
    """
    results = collection.query(
        query_texts=[query_text],
        n_results=3  
    )
    return results