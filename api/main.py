from fastapi import FastAPI
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    dbname=os.getenv("POSTGRES_DB", "metrics"),
    user=os.getenv("POSTGRES_USER", "app"),
    password=os.getenv("POSTGRES_PASSWORD", "app"),
)

@app.get("/metrics/latest")
def get_latest_metrics():
  with conn.cursor(cursor_factory=RealDictCursor) as cur:
    cur.execute("""
      SELECT *
      FROM public.metrics_windowed
      ORDER BY window_end DESC
      LIMIT 20;
    """)
    rows = cur.fetchall()
  return {"data": rows}
