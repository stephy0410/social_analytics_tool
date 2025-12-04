from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError # Importación específica del error
from bson import ObjectId
import bcrypt
import csv
from datetime import datetime


class MongoDBManager:
    def __init__(self, uri="mongodb://root:example@localhost:27017/", db_name="social_analytics"):
        
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

        # Collections
        self.users = self.db["users"]
        self.profiles = self.db["profiles"]

    
    # INDEXES
    
    def create_indexes(self):
        print("Creating indexes...")

        # USERS
        self.users.create_index("email", unique=True)
        self.users.create_index("username", unique=True)
        self.users.create_index("account_status")

        # PROFILES
        self.profiles.create_index("username", unique=True)
        self.profiles.create_index("linked_social_accounts.platform_name")

        print("Indexes created successfully!")

    
    # LOAD USERS FROM CSV 
    
    def load_users_from_csv(self, filepath="users.csv"):
        print("Loading users from CSV...")

        with open(filepath, newline='', encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                raw_id = row["user_id"]
                username = raw_id
               
                email = f"{raw_id}@example.com" 
                full_name = raw_id.replace("_", " ").title()

                user_id = ObjectId()

                password_hash = bcrypt.hashpw("default123".encode(), bcrypt.gensalt())

                user_doc = {
                    "_id": user_id,
                    "username": username,
                    "email": email,
                    "password_hash": password_hash.decode(),
                    "account_status": "active",
                    "timestamps": {
                        "created_at": datetime.utcnow(),
                        "modified_at": datetime.utcnow()
                    }
                }

                profile_doc = {
                    "_id": user_id,
                    "username": username,
                    "full_name": full_name,
                    "age": 25,
                    "gender": "unknown",
                    "bio": "",
                    "profile_picture_url": None,
                    "linked_social_accounts": []
                }

           
                try:
                    
                    self.users.insert_one(user_doc)
                    self.profiles.insert_one(profile_doc)
                except DuplicateKeyError:
                    
                    print(f"Skipping existing user: {username} ({email}). Already imported.")
                except Exception as e:
                    
                    print(f"An unexpected error occurred for user {username}: {e}")

        print("Users imported successfully!")

    
    # CREATE USER
    
    def create_user(self, username, email, password, full_name, age, gender):
        if self.username_exists(username):
            raise ValueError("Username already exists")

        if self.email_exists(email):
            raise ValueError("Email already exists")

        user_id = ObjectId()

        hashed_pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt())

        user_doc = {
            "_id": user_id,
            "username": username,
            "email": email,
            "password_hash": hashed_pw.decode(),
            "account_status": "active",
            "timestamps": {
                "created_at": datetime.utcnow(),
                "modified_at": datetime.utcnow()
            }
        }

        profile_doc = {
            "_id": user_id,
            "username": username,
            "full_name": full_name,
            "age": age,
            "gender": gender,
            "bio": "",
            "profile_picture_url": None,
            "linked_social_accounts": []
        }

        self.users.insert_one(user_doc)
        self.profiles.insert_one(profile_doc)
        try:
            with open("users.csv", "a", newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([username])
                print(f"Usuario {username} agregado a users.csv")
        except Exception as e:
            print(f"Error al escribir en users.csv: {e}")
        return user_id

    
    # UPDATE PROFILE
    
    def update_profile(self, username, updates: dict):
        updates["timestamps.modified_at"] = datetime.utcnow()
        return self.profiles.update_one(
            {"username": username},
            {"$set": updates}
        )

    
    # PUBLIC READ
    
    def get_public_profile(self, username):
        return self.profiles.find_one({"username": username})

    
    # VALIDATIONS
    
    def username_exists(self, username):
        return self.users.find_one({"username": username}) is not None

    def email_exists(self, email):
        return self.users.find_one({"email": email}) is not None

    
    # ACCOUNT STATUS MANAGEMENT 
    
    def set_account_status(self, username: str, new_status: str):
        """Cambia el estado de la cuenta de un usuario (active, suspended, deleted)."""
        valid_statuses = ["active", "suspended", "deleted"]
        if new_status not in valid_statuses:
            raise ValueError(f"Invalid status: {new_status}. Must be one of {valid_statuses}")
            
        result = self.users.update_one(
            {"username": username},
            {
                "$set": {
                    "account_status": new_status,
                    "timestamps.modified_at": datetime.utcnow()
                }
            }
        )
        return result

    
    # LOGIN — SECURE WITH BCRYPT 
    
    def login(self, username, password):
        user = self.users.find_one({"username": username})

        if not user:
            return None  # user not found

        hashed = user["password_hash"].encode()

        if bcrypt.checkpw(password.encode(), hashed):
            
            if user.get('account_status') != 'active':
                raise PermissionError(f"Login failed: Account is {user.get('account_status', 'unknown').upper()}.")
                
            return user  #

        return None 
    
    
    # ADD SOCIAL ACCOUNT (with extended fields)
    
    def add_social_account(self, username, platform, handle, followers=0, posts=0, profile_url=None):
        account = {
            "platform_name": platform,
            "handle": handle,
            "followers": followers,
            "posts": posts,
            "profile_url": profile_url,
            "last_updated": datetime.utcnow()
        }

        return self.profiles.update_one(
            {"username": username},
            {"$push": {"linked_social_accounts": account}}
        )

    def remove_social_account(self, username, platform, handle):
        return self.profiles.update_one(
            {"username": username},
            {"$pull": {"linked_social_accounts": {
                "platform_name": platform,
                "handle": handle
            }}}
        )


    
    # DELETE USER
    
    def delete_user(self, username):
        
        self.users.delete_one({"username": username})
        self.profiles.delete_one({"username": username})
        return True

    
    # ANALYTICS
    
    def count_social_platform_usage(self):
        pipeline = [
            {"$unwind": "$linked_social_accounts"},
            {
                "$group": {
                    "_id": "$linked_social_accounts.platform_name",
                    "total_users": {"$sum": 1}
                }
            },
            {"$sort": {"total_users": -1}}
        ]
        return list(self.profiles.aggregate(pipeline))

    
    # INIT DB 
    
    def initialize_database(self):
        print("Initializing MongoDB database...")
        
        
        print("Clearing 'users' and 'profiles' collections...")
        self.users.delete_many({})
        self.profiles.delete_many({})
        
        self.create_indexes()
        print("MongoDB structure ready!")

    def sync_mongo_to_csv(self, filepath="users.csv"):

            print(f"Initializing sincronization from MongoDB to {filepath}...")
            try:
                cursor = self.users.find({}, {"username": 1, "_id": 0})
                
                
                with open(filepath, "w", newline='', encoding="utf-8") as csvfile:
                    writer = csv.writer(csvfile)
                    
                    writer.writerow(["user_id"])
                
                    count = 0
                    for user_doc in cursor:
                        username = user_doc.get("username")
                        if username:
                            writer.writerow([username])
                            count += 1
                            
                
                
            except Exception as e:
                print(f" Error : {e}")

# RUN STANDALONE
if __name__ == "__main__":
    db = MongoDBManager()

    print("--Initializing data exporation")
    db.sync_mongo_to_csv("users.csv")
    print("--Completed Exportation")