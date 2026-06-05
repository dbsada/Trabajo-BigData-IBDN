import psycopg2, os

conn_str = os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'].replace('postgresql+psycopg2://', 'postgresql://')
conn = psycopg2.connect(conn_str)
cur = conn.cursor()

cur.execute("INSERT INTO ab_role (id, name) VALUES (1,'Admin'),(2,'User'),(3,'Op'),(4,'Viewer'),(5,'Public') ON CONFLICT DO NOTHING")
conn.commit()
conn.close()
print("Roles created")
