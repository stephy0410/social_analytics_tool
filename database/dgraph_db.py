import pydgraph
from datetime import datetime
import logging
import csv
import json
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONNECTION ---
def create_client_stub():
    return pydgraph.DgraphClientStub('localhost:9080')

def create_client(stub):
    return pydgraph.DgraphClient(stub)

# --- SCHEMA ---
def create_schema(client):
    schema = """
        user_id: string @index(exact) .
        post_id: string @index(exact) .
        author_id: string .
        content: string .
        timestamp: datetime @index(hour) .
        relationship_strength: float @index(float) .
        interaction_type: string .
        follower_id: string .
        followed_id: string .
        
        FOLLOWS: [uid] @reverse .
        LIKED_POST: [uid] @reverse .
        COMMENTED_POST: [uid] @reverse .
        SHARED_POST: [uid] @reverse .
        POSTED_BY: [uid] @reverse .

        type User {
            user_id
            FOLLOWS
            LIKED_POST
            COMMENTED_POST
            SHARED_POST
            POSTED_BY
        }
        type Post {
            post_id
            author_id
            content
            POSTED_BY
            LIKED_POST
            COMMENTED_POST
            SHARED_POST
        }
        type Interaction {
            user_id
            post_id
            interaction_type
            timestamp
        }
    """
    op = pydgraph.Operation(schema=schema)
    client.alter(op)
    logger.info("Schema created successfully")
# --- IMPROVED UID LOOKUP ---
def get_uid(client, identifier, is_post=False):
    """More robust UID lookup that handles both existing and new nodes"""
    if not identifier:
        return None
        
    field = "post_id" if is_post else "user_id"
    query = f"""{{
        u(func: eq({field}, "{identifier}")) {{
            uid
            {field}
        }}
    }}"""
    try:
        res = client.txn(read_only=True).query(query)
        data = json.loads(res.json)
        if data.get('u') and len(data['u']) > 0:
            return data['u'][0]['uid']
        
        # If not found, create the node
        return create_missing_node(client, identifier, is_post)
    except Exception as e:
        logger.error(f"UID lookup error for {identifier}: {e}")
        return None

def create_missing_node(client, identifier, is_post=False):
    """Create missing user/post node on the fly"""
    txn = client.txn()
    try:
        node_type = "Post" if is_post else "User"
        field = "post_id" if is_post else "user_id"
        
        mutation = {
            'uid': f'_:{identifier}',
            field: identifier,
            'dgraph.type': node_type
        }
        
        resp = txn.mutate(set_obj=mutation)
        txn.commit()
        logger.info(f"Created missing {node_type.lower()}: {identifier}")
        return resp.uids.get(identifier)
    except Exception as e:
        logger.error(f"Error creating missing node: {e}")
        return None
    finally:
        txn.discard()

# --- CSV PERSISTENCE FUNCTIONS ---
def save_interaction_to_csv(user_id, post_id, interaction_type, file_path='interactions.csv'):
    """Append new interaction to CSV file"""
    try:
        # Check if file exists
        file_exists = os.path.isfile(file_path)
        
        # Normalize interaction type
        interaction_map = {
            "Like": "LIKED_POST",
            "Comment": "COMMENTED_POST",
            "Share": "SHARED_POST"
        }
        csv_interaction_type = interaction_map.get(interaction_type, interaction_type)
        
        with open(file_path, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['user_id', 'post_id', 'interaction_type', 'timestamp']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header if file is new
            if not file_exists:
                writer.writeheader()
            
            writer.writerow({
                'user_id': user_id,
                'post_id': post_id,
                'interaction_type': csv_interaction_type,
                'timestamp': datetime.now().isoformat()
            })
        
        logger.info(f"Saved interaction to CSV: {user_id} -> {post_id} ({csv_interaction_type})")
        return True
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        return False

def save_follow_to_csv(follower_id, followed_id, file_path='follows.csv'):
    """Append new follow relationship to CSV file"""
    try:
        file_exists = os.path.isfile(file_path)
        
        with open(file_path, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['follower_id', 'followed_id', 'timestamp']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerow({
                'follower_id': follower_id,
                'followed_id': followed_id,
                'timestamp': datetime.now().isoformat()
            })
        
        logger.info(f"Saved follow to CSV: {follower_id} -> {followed_id}")
        return True
    except Exception as e:
        logger.error(f"Error saving follow to CSV: {e}")
        return False

# --- IMPROVED DATA LOADERS ---
def load_users_from_csv(client, file_path='users.csv'):
    txn = client.txn()
    try:
        users = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader: 
                row = {k.strip(): v for k, v in row.items()}
                if 'user_id' in row and row['user_id']:
                    users.append({
                        'uid': '_:' + row['user_id'],
                        'user_id': row['user_id'],
                        'dgraph.type': 'User'
                    })
        if users:
            resp = txn.mutate(set_obj=users)
            txn.commit()
            logger.info(f"Loaded {len(users)} users")
            return resp.uids
        return {}
    except Exception as e:
        logger.error(f"Error loading users: {e}")
        return {}
    finally:
        txn.discard()

def load_posts_from_csv(client, file_path='posts.csv'):
    txn = client.txn()
    try:
        posts = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader: 
                row = {k.strip(): v for k, v in row.items()}
                
                if 'post_id' not in row or not row['post_id']:
                    continue
                    
                # Handle different author field names
                author_id = row.get('user_id') or row.get('author_id')
                post_data = {
                    'uid': '_:' + row['post_id'],
                    'post_id': row['post_id'],
                    'dgraph.type': 'Post'
                }
                
                # Link post to author if author exists
                if author_id:
                    author_uid = get_uid(client, author_id)
                    if author_uid:
                        post_data['POSTED_BY'] = {'uid': author_uid}
                
                posts.append(post_data)
        
        if posts:
            resp = txn.mutate(set_obj=posts)
            txn.commit()
            logger.info(f"Loaded {len(posts)} posts")
            return resp.uids
        return {}
    except Exception as e:
        logger.error(f"Error loading posts: {e}")
        return {}
    finally:
        txn.discard()

def load_interactions_from_csv(client, file_path='interactions.csv'):
    txn = client.txn()
    try:
        success_count = 0
        skipped_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row_num, row in enumerate(reader, start=2):
                try:
                    row = {k.strip(): v.strip() for k, v in row.items() if k}
                    
                    user_id = row.get('user_id', '').strip()
                    post_id = row.get('post_id', '').strip()
                    interaction_type = row.get('interaction_type', '').upper().strip()
                    
                    if not all([user_id, post_id, interaction_type]):
                        logger.warning(f"Row {row_num}: Missing required fields, skipping")
                        skipped_count += 1
                        continue
                    
                    # Convert interaction type to edge name
                    edge_map = {
                        'LIKE': 'LIKED_POST',
                        'COMMENT': 'COMMENTED_POST', 
                        'SHARE': 'SHARED_POST',
                        'LIKED_POST': 'LIKED_POST',
                        'COMMENTED_POST': 'COMMENTED_POST',
                        'SHARED_POST': 'SHARED_POST'
                    }
                    
                    edge_name = edge_map.get(interaction_type)
                    if not edge_name:
                        logger.warning(f"Row {row_num}: Invalid interaction type '{interaction_type}', skipping")
                        skipped_count += 1
                        continue
                    
                    user_uid = get_uid(client, user_id)
                    post_uid = get_uid(client, post_id, is_post=True)
                    
                    if user_uid and post_uid:
                        mutation = {
                            'uid': user_uid,
                            edge_name: {
                                'uid': post_uid, 
                                'timestamp': datetime.now().isoformat()
                            }
                        }
                        txn.mutate(set_obj=mutation)
                        success_count += 1
                    else:
                        logger.warning(f"Row {row_num}: Could not find UIDs for {user_id} -> {post_id}")
                        skipped_count += 1
                        
                except Exception as row_error:
                    logger.error(f"Row {row_num}: Error processing row: {row_error}")
                    skipped_count += 1
                    continue
        
        txn.commit()
        logger.info(f"Loaded {success_count} interactions ({skipped_count} skipped)")
        return success_count
    except Exception as e:
        logger.error(f"Error loading interactions: {e}")
        return 0
    finally:
        txn.discard()
def create_follow_edges(client, file_path='follows.csv'):
    txn = client.txn()
    try:
        success_count = 0
        skipped_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row_num, row in enumerate(reader, start=2):
                try:
                    row = {k.strip(): v.strip() for k, v in row.items() if k}
                    follower_id = row.get('follower_id', '').strip()
                    followed_id = row.get('followed_id', '').strip()
                    
                    if not follower_id or not followed_id:
                        skipped_count += 1
                        continue
                    
                    if '\n' in follower_id or '\n' in followed_id or ',' in follower_id:
                        skipped_count += 1
                        continue
                    
                    follower_uid = get_uid(client, follower_id)
                    followed_uid = get_uid(client, followed_id)
                    
                    if follower_uid and followed_uid:
                        # Create edge with facets
                        mutation = {
                            'uid': follower_uid,
                            'FOLLOWS': {
                                'uid': followed_uid,
                                'FOLLOWS|timestamp': row.get('timestamp', datetime.now().isoformat()),
                                'FOLLOWS|relationship_strength': 0.1
                            }
                        }
                        txn.mutate(set_obj=mutation)
                        success_count += 1
                    else:
                        skipped_count += 1
                        
                except Exception as row_error:
                    logger.error(f"Row {row_num}: Error: {row_error}")
                    skipped_count += 1
                    continue
        
        txn.commit()
        logger.info(f"Created {success_count} follow relationships with timestamps")
        return success_count
    except Exception as e:
        logger.error(f"Error creating follow edges: {e}")
        return 0
    finally:
        txn.discard()
# --- IMPROVED REAL-TIME UPDATES WITH CSV PERSISTENCE ---
def add_realtime_interaction(client, user_id, post_id, interaction_type):
    """Fixed real-time interaction with CSV persistence"""
    if not user_id or not post_id:
        return False, "User ID and Post ID are required"
    
    user_uid = get_uid(client, user_id)
    post_uid = get_uid(client, post_id, is_post=True)
    
    if not user_uid:
        return False, f"User '{user_id}' not found"
    if not post_uid:
        return False, f"Post '{post_id}' not found"

    edge_map = {
        "Like": "LIKED_POST",
        "Comment": "COMMENTED_POST", 
        "Share": "SHARED_POST"
    }
    edge = edge_map.get(interaction_type)
    
    if not edge:
        return False, f"Invalid interaction type: {interaction_type}"

    txn = client.txn()
    try:
        mutation = {
            'uid': user_uid,
            edge: {
                'uid': post_uid, 
                'timestamp': datetime.now().isoformat()
            }
        }
        txn.mutate(set_obj=mutation)
        txn.commit()
        
        # Save to CSV
        save_interaction_to_csv(user_id, post_id, interaction_type)
        
        # Update relationship strength after interaction
        compute_relationship_strength(client, user_id)
        
        return True, f"Success: {user_id} {interaction_type}d post {post_id}"
    except Exception as e:
        logger.error(f"Real-time interaction error: {e}")
        return False, f"Database error: {str(e)}"
    finally:
        txn.discard()

def add_realtime_follow(client, follower_id, followed_id):
    if not follower_id or not followed_id:
        return False, "Follower and followed IDs are required"
    
    if follower_id == followed_id:
        return False, "Cannot follow yourself"
    
    follower_uid = get_uid(client, follower_id)
    followed_uid = get_uid(client, followed_id)
    
    if not follower_uid or not followed_uid:
        return False, "User not found"

    txn = client.txn()
    try:
        # --- FIX: Use 'FOLLOWS|key' syntax ---
        mutation = {
            'uid': follower_uid,
            'FOLLOWS': {
                'uid': followed_uid, 
                'FOLLOWS|timestamp': datetime.now().isoformat(),
                'FOLLOWS|relationship_strength': 0.5
            }
        }
        txn.mutate(set_obj=mutation)
        txn.commit()
        
        save_follow_to_csv(follower_id, followed_id)
        return True, f"Success: {follower_id} now follows {followed_id}"
    except Exception as e:
        logger.error(f"Real-time follow error: {e}")
        return False, f"Database error: {str(e)}"
    finally:
        txn.discard()
def get_engagement_metrics(client, user_id, start_date=None, end_date=None):
    """
    Fixed engagement metrics with DATE RANGE FILTERING.
    Format dates as ISO strings: '2023-01-01'
    """
    try:
        create_schema(client)
    except Exception:
        pass  
    # Base filter string
    date_filter = ""
    if start_date and end_date:
        # Dgraph syntax: @filter(ge(timestamp, "START") AND le(timestamp, "END"))
        date_filter = f'@filter(ge(timestamp, "{start_date}") AND le(timestamp, "{end_date}"))'
    
    query = f"""{{
      user(func: eq(user_id, "{user_id}")) {{
        user_id
        posts: ~POSTED_BY {{
          post_id
          likes: count(~LIKED_POST {date_filter})
          comments: count(~COMMENTED_POST {date_filter}) 
          shares: count(~SHARED_POST {date_filter})
        }}
      }}
    }}"""
    
    try:
        res = client.txn(read_only=True).query(query)
        data = json.loads(res.json)
        
        if data.get('user') and len(data['user']) > 0:
            user_data = data['user'][0]
            posts = user_data.get('posts', [])
            
            total_likes = sum(p.get('likes', 0) for p in posts)
            total_comments = sum(p.get('comments', 0) for p in posts)
            total_shares = sum(p.get('shares', 0) for p in posts)
            
            return {
                'posts': posts,
                'total_likes': total_likes,
                'total_comments': total_comments, 
                'total_shares': total_shares,
                'post_count': len(posts)
            }
        return {'posts': [], 'total_likes': 0, 'total_comments': 0, 'total_shares': 0, 'post_count': 0}
    except Exception as e:
        logger.error(f"Engagement metrics error: {e}")
        return {'posts': [], 'total_likes': 0, 'total_comments': 0, 'total_shares': 0, 'post_count': 0}

def get_following(client, user_id):
    
    query = f"""{{
      user(func: eq(user_id, "{user_id}")) {{
        following: FOLLOWS {{ 
            user_id, 
            relationship_strength
            secondary: FOLLOWS {{
                user_id
            }}
        }}
      }}
    }}"""
    res = client.txn(read_only=True).query(query)
    d = json.loads(res.json)
    return d['user'][0]['following'] if d.get('user') else []
def get_followers(client, user_id):
    query = f"""{{
      user(func: eq(user_id, "{user_id}")) {{
        followers: ~FOLLOWS {{ user_id }}
      }}
    }}"""
    res = client.txn(read_only=True).query(query)
    d = json.loads(res.json)
    return d['user'][0]['followers'] if d.get('user') else []

def get_mutual_connections(client, user_id):
    query = f"""{{
      var(func: eq(user_id, "{user_id}")) {{ u as uid }}
      user(func: uid(u)) {{
        mutuals: FOLLOWS @filter(uid_in(~FOLLOWS, uid(u))) {{ user_id }}
      }}
    }}"""
    res = client.txn(read_only=True).query(query)
    d = json.loads(res.json)
    return d['user'][0]['mutuals'] if d.get('user') else []

def get_influencers(client, min_followers=3):
    query = f"""{{
      influencers(func: type(User)) @filter(ge(count(~FOLLOWS), {min_followers})) {{
        user_id
        follower_count: count(~FOLLOWS) 
      }}
    }}"""
    res = client.txn(read_only=True).query(query)
    d = json.loads(res.json)
    return sorted(d.get('influencers', []), key=lambda x: x.get('follower_count', 0), reverse=True)

def find_shortest_path(client, src_user_id, dst_user_id):
    src_uid = get_uid(client, src_user_id)
    dst_uid = get_uid(client, dst_user_id)
    if not src_uid or not dst_uid: return []
    query = f"""{{
        path as shortest(from: {src_uid}, to: {dst_uid}) {{ FOLLOWS }}
        path(func: uid(path)) {{ user_id }}
    }}"""
    res = client.txn(read_only=True).query(query)
    return json.loads(res.json).get('path', [])

def get_all_users(client):
    query = """{ all_users(func: type(User)) { user_id } }"""
    try:
        res = client.txn(read_only=True).query(query)
        return json.loads(res.json).get('all_users', [])
    except: return []

def drop_all_data(client):
    """Fixed drop all data function"""
    try:
        op = pydgraph.Operation(drop_all=True)
        client.alter(op)
        logger.info("Successfully dropped all data from database")
        return True
    except Exception as e:
        logger.error(f"Error dropping data: {e}")
        return False
def get_community_clusters(client, current_user_id=None):
    """
    Finds triangles specifically involving the current user.
    Path: User -> Friend (A) -> Friend of Friend (B) -> User
    """
    if not current_user_id:
        return []

    query = f"""{{
      var(func: eq(user_id, "{current_user_id}")) {{ root as uid }}

      clusters(func: uid(root)) {{
        # Level 1: Who do I follow? (e.g., Amazon Recruiter)
        L1: FOLLOWS {{
          user_id
          
          # Level 2: Who do they follow? (e.g., Hannah)
          L2: FOLLOWS {{
            user_id
            
            # Level 3: Does that person follow ME back? (Closing the loop)
            L3: FOLLOWS @filter(uid(root)) {{
                user_id
            }}
          }}
        }}
      }}
    }}"""
    
    try:
        res = client.txn(read_only=True).query(query)
        data = json.loads(res.json)
        
        found_clusters = []
        root_user = current_user_id
        
        # Parse the nested layers
        if data.get('clusters'):
            for friend_a in data['clusters'][0].get('L1', []):
                for friend_b in friend_a.get('L2', []):
                    # Check if Level 3 exists (meaning Friend B follows Root)
                    if friend_b.get('L3'):
                        # We found a triangle: Root -> A -> B -> Root
                        # Sort names to avoid duplicates (A-B-C vs C-B-A)
                        cluster = sorted([root_user, friend_a['user_id'], friend_b['user_id']])
                        
                        if cluster not in found_clusters:
                            found_clusters.append(cluster)
                                
        return found_clusters
    except Exception as e:
        logger.error(f"Cluster error: {e}")
        return []
def compute_relationship_strength(client, user_id):
    """
    Computes strength based on interaction frequency.
    Formula: Base (0.1) + (Likes * 0.2) + (Comments * 0.5) + (Shares * 1.0)
    """
    query = f"""{{
        user(func: eq(user_id, "{user_id}")) {{
            uid
            following: FOLLOWS {{
                followed_uid as uid
                user_id
            }}
        }}
    }}"""
    
    txn = client.txn()
    try:
        res = client.txn(read_only=True).query(query)
        data = json.loads(res.json)
        
        if not data.get('user'): 
            return

        user_uid = data['user'][0]['uid']
        following = data['user'][0].get('following', [])
        
        mutations = []
        for friend in following:
            friend_uid = friend['uid']
            friend_id = friend['user_id']
            
            # Count interactions with this friend's posts
            interaction_query = f"""{{
                user(func: eq(user_id, "{user_id}")) {{
                    likes: count(LIKED_POST @filter(uid_in(~POSTED_BY, {friend_uid})))
                    comments: count(COMMENTED_POST @filter(uid_in(~POSTED_BY, {friend_uid})))
                    shares: count(SHARED_POST @filter(uid_in(~POSTED_BY, {friend_uid})))
                }}
            }}"""
            
            interaction_res = client.txn(read_only=True).query(interaction_query)
            interaction_data = json.loads(interaction_res.json)
            
            if interaction_data.get('user'):
                user_data = interaction_data['user'][0]
                likes = user_data.get('likes', 0)
                comments = user_data.get('comments', 0)
                shares = user_data.get('shares', 0)
                
                # Calculate Score (Max 5.0)
                raw_score = 0.1 + (likes * 0.2) + (comments * 0.5) + (shares * 1.0)
                final_score = min(raw_score, 5.0)
                
                # Update the edge facet
                mutations.append({
                    'uid': user_uid,
                    'FOLLOWS': {
                        'uid': friend_uid,
                        'FOLLOWS|relationship_strength': final_score
                    }
                })
            
        if mutations:
            txn.mutate(set_obj=mutations)
            txn.commit()
            logger.info(f"Updated relationship strengths for {user_id}")
            
    except Exception as e:
        logger.error(f"Error computing strength: {e}")
    finally:
        txn.discard()



def get_follower_growth(client, user_id):
    """
    Returns a list of {user_id, timestamp} for every user that follows *user_id*.
    The timestamp is read from the facet on the ~FOLLOWS edge.
    """
    query = f"""{{
      user(func: eq(user_id, "{user_id}")) {{
        followers: ~FOLLOWS @facets(timestamp) {{
          user_id
        }}
      }}
    }}"""

    try:
        res = client.txn(read_only=True).query(query)
        data = json.loads(res.json)
        logger.info(f"RAW follower response for {user_id}: {data}")
        followers = data.get('user', [{}])[0].get('followers', [])
        result = []

        for f in followers:
            ts = f.get('followers|timestamp')
            if ts:
                result.append({'user_id': f['user_id'], 'timestamp': ts})

        return result
    except Exception as e:
        logger.error(f"get_follower_growth error: {e}")
        return []