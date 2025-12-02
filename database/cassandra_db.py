from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import pandas as pd
from datetime import datetime


class CassandraDB:
    def __init__(self, host="127.0.0.1", keyspace="social"):
        self.cluster = Cluster([host])
        self.session = self.cluster.connect()

        self.keyspace = keyspace
        self._create_keyspace()
        self.session.set_keyspace(keyspace)

        self._init_table()


    # -----------------------------
    #  KEYSPACE
    # -----------------------------
    def _create_keyspace(self):
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }};
        """)

    # -----------------------------
    #  MAIN TABLE
    # -----------------------------
    def _init_table(self):
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS interactions (
                user_id TEXT,
                post_id TEXT,
                interaction_type TEXT,
                timestamp TIMESTAMP,
                device_type TEXT,
                session_id TEXT,
                PRIMARY KEY (user_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC);
        """)

    # -----------------------------
    # INSERT ROW
    # -----------------------------
    def insert_interaction(self, user_id, post_id, interaction_type,
                           timestamp, device_type, session_id):

        query = """
            INSERT INTO interactions (
                user_id, post_id, interaction_type,
                timestamp, device_type, session_id
            ) VALUES (%s, %s, %s, %s, %s, %s);
        """

        self.session.execute(query, (
            user_id, post_id, interaction_type,
            timestamp, device_type, session_id
        ))


    # -----------------------------
    # LOAD CSV INTO CASSANDRA
    # -----------------------------
    def load_csv(self, csv_path):
        df = pd.read_csv(csv_path)

        for _, row in df.iterrows():
            ts = pd.to_datetime(row["timestamp"])

            self.insert_interaction(
                row["user_id"],
                row["post_id"],
                row["interaction_type"],
                ts,
                row.get("device_type", "unknown"),
                row.get("session_id", "none")
            )

        return True


    # -----------------------------
    # BASIC QUERY: BY USER
    # -----------------------------
    def get_interactions_by_user(self, user_id, limit=50):
        query = """
            SELECT * FROM interactions
            WHERE user_id = %s
            LIMIT %s;
        """

        rows = self.session.execute(query, (user_id, limit))

        df = pd.DataFrame(rows)
        return df.sort_values("timestamp", ascending=False)


    # -----------------------------
    # RANGE QUERY (RF-3)
    # -----------------------------
    def get_interactions_by_time_range(self, user_id, start_date, end_date):
        query = """
            SELECT * FROM interactions
            WHERE user_id = %s
            AND timestamp >= %s
            AND timestamp <= %s;
        """

        rows = self.session.execute(query, (user_id, start_date, end_date))
        df = pd.DataFrame(rows)
        return df.sort_values("timestamp", ascending=False)


    # -----------------------------
    # DAILY AGGREGATION (RF-2, RF-7)
    # -----------------------------
    def get_daily_activity_count(self, user_id):
        query = """
            SELECT user_id, timestamp
            FROM interactions
            WHERE user_id = %s;
        """

        rows = self.session.execute(query, (user_id,))

        df = pd.DataFrame(rows)
        if df.empty:
            return df

        df["date"] = df["timestamp"].dt.date
        return df.groupby("date").size().reset_index(name="count")


    # -----------------------------
    # INACTIVITY PATTERNS (RF-6)
    # -----------------------------
    def compute_inactivity_periods(self, user_id, days_threshold=7):
        df = self.get_interactions_by_user(user_id, limit=500)

        if df.empty:
            return pd.DataFrame()

        df = df.sort_values("timestamp")
        df["delta"] = df["timestamp"].diff().dt.days

        return df[df["delta"] >= days_threshold]


    # -----------------------------
    # ANOMALY DETECTION (RF-5)
    # -----------------------------
    def detect_abnormal_activity(self, user_id, period="day"):
        df = self.get_daily_activity_count(user_id)

        if df.empty:
            return df, None, None

        mean = df["count"].mean()
        std = df["count"].std()

        spike_threshold = mean + 2 * std
        drop_threshold = mean - 2 * std

        df["status"] = df["count"].apply(
            lambda x: "SPIKE" if x > spike_threshold
            else ("DROP" if x < drop_threshold else "normal")
        )

        return df, spike_threshold, drop_threshold
