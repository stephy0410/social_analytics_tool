from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
from datetime import datetime
import csv
import os

class CassandraDB:
    def __init__(self, host="localhost", port=9042):
        auth_provider = PlainTextAuthProvider(
            username="cassandra",
            password="cassandra",
        )
        cluster = Cluster([host], port=port, auth_provider=auth_provider)

        self.session = cluster.connect()
        self.session.row_factory = dict_factory

        self._init_keyspace()
        self._init_table()

    # ------------------- init schema -------------------

    def _init_keyspace(self):
        self.session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS social
            WITH replication = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            };
            """
        )
        self.session.set_keyspace("social")

    def _init_table(self):
        # NUEVA TABLA basándose en interactions.csv
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS interactions (
                user_id TEXT,
                post_id TEXT,
                interaction_type TEXT,
                timestamp TIMESTAMP,
                PRIMARY KEY (user_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC);
            """
        )

    # ------------------- inserts -----------------------

    def insert_interaction(self, user_id, post_id, interaction_type, timestamp=None):
        if timestamp is None:
            timestamp = datetime.now()

        self.session.execute(
            """
            INSERT INTO interactions (user_id, post_id, interaction_type, timestamp)
            VALUES (%s, %s, %s, %s)
            """,
            (user_id, post_id, interaction_type, timestamp),
        )

    def load_interactions_from_csv(self, csv_path: str = "interactions.csv"):
        """Carga interactions.csv con columnas: user_id, post_id, interaction_type, timestamp"""
        if not os.path.exists(csv_path):
            print(f"CSV not found: {csv_path}")
            return

        print("Importando interactions.csv a Cassandra...")

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts = row.get("timestamp")
                if ts:
                    try:
                        ts = datetime.fromisoformat(ts)
                    except ValueError:
                        ts = datetime.now()
                else:
                    ts = datetime.now()

                self.insert_interaction(
                    row["user_id"],
                    row["post_id"],
                    row["interaction_type"],
                    ts,
                )

        print("Cassandra: interactions CSV importado correctamente.")

    # ------------------- queries -----------------------

    def get_interactions_by_user(self, user_id: str, limit: int = 50):
        """Devuelve interactions ordenadas por timestamp DESC."""
        rows = self.session.execute(
            """
            SELECT user_id, post_id, interaction_type, timestamp
            FROM interactions
            WHERE user_id = %s
            LIMIT %s
            """,
            (user_id, limit),
        )
        return list(rows)

    def get_interactions_by_type(self, user_id: str, interaction_type: str, limit: int = 50):
        """Filtra por interaction_type."""
        rows = self.session.execute(
            """
            SELECT user_id, post_id, interaction_type, timestamp
            FROM interactions
            WHERE user_id = %s AND interaction_type = %s
            LIMIT %s
            ALLOW FILTERING
            """,
            (user_id, interaction_type, limit),
        )
        return list(rows)
