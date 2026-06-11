import json
import os
import time

from cassandra.cluster import Cluster

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
DATA_FILE = os.getenv("DATA_FILE", "/app/data/origin_dest_distances.jsonl")


def main():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()

    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS agile_data_science "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("agile_data_science")

    session.execute(
        "CREATE TABLE IF NOT EXISTS origin_dest_distances ("
        "origin text, dest text, distance double, PRIMARY KEY (origin, dest)"
        ")"
    )
    session.execute("TRUNCATE origin_dest_distances")

    session.execute(
        "CREATE TABLE IF NOT EXISTS flight_delay_ml_response ("
        "uuid text PRIMARY KEY, prediction int, model_id text, "
        "origin text, dest text, "
        "dep_delay double, carrier text, flight_date text, flight_num text, "
        "distance double, route text, day_of_year int, day_of_month int, "
        "day_of_week int, timestamp text)"
    )

    n = 0
    with open(DATA_FILE) as f:
        for line in f:
            r = json.loads(line)
            session.execute(
                "INSERT INTO origin_dest_distances (origin, dest, distance) "
                "VALUES (%s, %s, %s)",
                (r["Origin"], r["Dest"], r["Distance"]),
            )
            n += 1

    print(f"{n} records")


if __name__ == "__main__":
    main()
