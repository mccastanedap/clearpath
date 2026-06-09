import os
import psycopg2

conn = psycopg2.connect(
    host=os.environ["SUPABASE_HOST"],
    port=os.environ["SUPABASE_PORT"],
    user=os.environ["SUPABASE_USER"],
    password=os.environ["SUPABASE_PASSWORD"],
    dbname=os.environ["SUPABASE_DATABASE"],
    sslmode="require",
)
cur = conn.cursor()
cur.execute("SELECT 1;")
print("Keepalive OK:", cur.fetchone())
cur.close()
conn.close()
