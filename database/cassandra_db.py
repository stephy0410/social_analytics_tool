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
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS activity_logs (
                user_id TEXT,
                action  TEXT,
                timestamp TIMESTAMP,
                PRIMARY KEY (user_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC);
            """
        )

    # ------------------- inserts -----------------------

    def insert_log(self, user_id: str, action: str, timestamp=None):
        if timestamp is None:
            timestamp = datetime.now()

        self.session.execute(
            """
            INSERT INTO activity_logs (user_id, action, timestamp)
            VALUES (%s, %s, %s)
            """,
            (user_id, action, timestamp),
        )

    def seed_demo_data(self):
        """Inserta algunos registros de ejemplo si la tabla está vacía."""
        count = self.session.execute("SELECT COUNT(*) FROM activity_logs").one()["count"]
        if count > 0:
            return

        print("Seeding Cassandra with demo activity logs...")


    def load_demo_from_csv(self, csv_path: str = "activity_logs.csv"):
        """Opcional: carga actividades desde un CSV (user_id, action, timestamp)."""
        if not os.path.exists(csv_path):
            print(f"CSV not found: {csv_path}")
            return

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

                self.insert_log(row["user_id"], row["action"], ts)

        print("CSV logs imported successfully.")

    # ------------------- queries -----------------------

    def get_activity_by_user(self, user_id: str, limit: int = 50):
        """Devuelve todas las actividades de un usuario (para el dashboard)."""
        rows = self.session.execute(
            """
            SELECT user_id, action, timestamp
            FROM activity_logs
            WHERE user_id = %s
            LIMIT %s
            """,
            (user_id, limit),
        )
        return list(rows)

    def get_activities_by_type(self, user_id: str, action: str, limit: int = 50):
        """Filtra actividades por tipo (action)."""
        rows = self.session.execute(
            """
            SELECT user_id, action, timestamp
            FROM activity_logs
            WHERE user_id = %s AND action = %s
            LIMIT %s
            ALLOW FILTERING
            """,
            (user_id, action, limit),
        )
        return list(rows)

