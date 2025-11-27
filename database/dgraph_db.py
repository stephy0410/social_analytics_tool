import pydgraph
from datetime import datetime
import logging
import csv
import json

# Set logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Connection and Schema
client_stub = pydgraph.DgraphClientStub('localhost:9080')
client = pydgraph.DgraphClient(client_stub)
logger.info(f"Connected to Dgraph at localhost:9080")

def create_schema(client):
    schema = """
        user_id: string @index(exact) .
        post_id: string @index(exact) .

        FOLLOWS: [uid] @reverse .
        LIKED_POST: [uid] @reverse .
        COMMENTED_POST: [uid] @reverse .
        SHARED_POST: [uid] @reverse .

        timestamp: datetime @index(hour) .
        relationship_strength: float @index(float) .
        interaction_count: int .

        type User {
            user_id
            FOLLOWS
            LIKED_POST
            COMMENTED_POST
            SHARED_POST
        }
        type Post {
            post_id
        }
    """
    op = pydgraph.Operation(schema=schema)
    client.alter(op)
    logger.info("Schema created successfully")

#Data Loading
def load_users_from_csv(client, file_path = 'users_csv'):
    txn = client.txn()
    resp = None

    try:
        users = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader: 
                users.append({
                    'uid': '_:' + row['user_id'],
                    'user_id':row['user_id'],
                    'dgraph.type': 'User'
                })
        print(f"Loading users: {users}")
        resp = txn.mutate(set_obj=users)
        txn.commit()
        logger.info(f"Loaded {len(users)} users")
    finally:
        txn.discard()
    return resp.uids if resp else {}
def load_posts_from_csv(client, file_path = 'posts_csv'):
    txn = client.txn()
    resp = None

    try:
        posts = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader: 
                posts.append({
                    'uid': '_:' + row['post_id'],
                    'post_id':row['post_id'],
                    'dgraph.type': 'Post'
                })
        print(f"Loading posts: {posts}")
        resp = txn.mutate(set_obj=posts)
        txn.commit()
        logger.info(f"Loaded {len(posts)} posts")
    finally:
        txn.discard()
    return resp.uids if resp else {}
def create_follow_edges(client, file_path = 'follows_csv', user_uids=None):
    txn = client.txn()

    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader: 
                follower = row['follower_id']
                followed = row['followed_id']
                mutation = {
                    'uid': user_uids[follower],
                    'FOLLOWS':{
                        'uid':user_uids[followed],
                        'timestamp': datetime.now().isoformat()
                    }
                }
                print(f"Creating relationship: {follower} -FOLLOWS-> {followed}")
                txn.mutate(set_obj=mutation)
        txn.commit()
        logger.info("Follow relationships created")
    finally:
        txn.discard()
def create_interaction_edges(client, file_path = 'interactions_csv', user_uids=None, post_uids=None):
    txn = client.txn()

    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader: 
                user_id= row['user_id']
                post_id = row['post_id']
                interaction_type = row['interaction_type'] # LIKED_POST, COMMENTED_POST, etc.
                mutation = {
                    'uid': user_uids[user_id],
                    interaction_type:{
                        'uid':post_uids[post_id],
                        'timestamp': datetime.now().isoformat()
                    }
                }
                print(f"Creating interaction: {user_id} -{interaction_type}-> {post_id}")
                txn.mutate(set_obj=mutation)
        txn.commit()
        logger.info("Interactions created")
    finally:
        txn.discard()

#QUERIES
def get_followers(client, user_id):
    query = f"""{{
      user(func: eq(user_id, "{user_id}")) {{
        user_id
        followers: ~FOLLOWS {{
            user_id
        }}
      }}
    }}"""
    res = client.txn(read_only=True).query(query)
    result= json.loads(res.json)
    print(f"\nFollowers of {user_id}:")
    if result.get('user') and result['user']:
        followers = result['user'][0].get('followers', [])
        for follower in followers:
            print(f"  - {follower['user_id']}")
        return followers
    return []
def get_following(client, user_id):
    query = f"""{{
      user(func: eq(user_id, "{user_id}")) {{
        user_id
        following: FOLLOWS {{
            user_id
        }}
      }}
    }}"""
    res = client.txn(read_only=True).query(query)
    result= json.loads(res.json)
    print(f"\n{user_id} is following:")
    if result.get('user') and result['user']:
        following = result['user'][0].get('following', [])
        for followed in following:
            print(f"  - {followed['user_id']}")
        return following
    return []
def get_mutual_connections(client, user_id):
    query = f"""{{
      var(func: eq(user_id, "{user_id}")) {{
        user_uid as uid
      }}
      user(func: uid(user_id) {{
        user_id
        mutuals: FOLLOWS @filter(uid_in(~FOLLOWS,uid(user_uid))) {{
            user_id
        }}
      }}
    }}"""
    res = client.txn(read_only=True).query(query)
    result= json.loads(res.json)
    print(f"\nMutual connections of {user_id}:")
    if result.get('user') and result['user']:
        mutuals = result['user'][0].get('mutuals', [])
        for mutual in mutuals:
            print(f"  - {mutual['user_id']}")
        return mutuals
    return []
def get_user_posts_interactions(client, user_id):
    query = f"""{{
      user(func: eq(user_id, "{user_id}")) {{
        user_id
        liked_posts: LIKED_POST {{
            post_id
        }}
        commented_posts: COMMENTED_POST {{
            post_id
        }}
        shared_posts: SHARED_POST {{
            post_id
        }}
      }}
    }}"""
    res = client.txn(read_only=True).query(query)
    result= json.loads(res.json)
    print(f"\n{user_id}'s interactions:")
    if result.get('user') and result['user']:
        user_data = result['user'][0]
        print(f"  Liked: {len(user_data.get('liked_posts', []))} posts")
        print(f"  Commented: {len(user_data.get('commented_posts', []))} posts")
        print(f"  Shared: {len(user_data.get('shared_posts', []))} posts")
    
    return result
def get_posts_engagement(client, post_id):
    query = f"""{{
      post(func: eq(post_id, "{post_id}")) {{
        post_id
        liked_by: ~LIKED_POST {{
            user_id
        }}
        commented_by: ~COMMENTED_POST {{
            user_id
        }}
        shared_by: ~SHARED_POST {{
            user_id
        }}
      }}
    }}"""
    res = client.txn(read_only=True).query(query)
    result= json.loads(res.json)
    print(f"\nEngagement for {post_id}:")
    if result.get('post') and result['post']:
        post_data = result['post'][0]
        print(f"  Likes: {len(post_data.get('liked_by', []))}")
        print(f"  Comments: {len(post_data.get('commented_by', []))}")
        print(f"  Shares: {len(post_data.get('shared_by', []))}")
    
    return result
def get_influencers(client, min_followers=1000):
    query = f"""{{
      influencers(func: type(User)) @filter(ge(count(~FOLLOWS), {min_followers})) {{
        user_id
        follower_count: count(~FOLLOWS) 
      }}
    }}"""
    res = client.txn(read_only=True).query(query)
    result= json.loads(res.json)
    print(f"\nInfluencers (>= {min_followers} followers):")
    if result.get('influencers'):
        # Sort by follower count
        influencers = sorted(result['influencers'], 
                           key=lambda x: x.get('follower_count', 0), 
                           reverse=True)
        for inf in influencers:
            print(f"  - {inf['user_id']}: {inf['follower_count']} followers")
        return influencers
    return []
# Add this to database/dgraph_db.py

def get_all_users(client):
    query = """{
        all_users(func: type(User)) {
            user_id
        }
    }"""
    try:
        res = client.txn(read_only=True).query(query)
        data = json.loads(res.json)
        # Returns a list like: [{'user_id': 'hannah_chenoa'}, {'user_id': 'luis_fer'}]
        return data.get('all_users', [])
    except Exception as e:
        print(f"Error fetching users: {e}")
        return []
