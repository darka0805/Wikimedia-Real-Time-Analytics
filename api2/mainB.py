from fastapi import FastAPI, Query
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from dateutil.parser import isoparse
from typing import Optional
import redis
import json

app = FastAPI()

CASSANDRA_NODES = ["cassandra"]
CASSANDRA_KEYSPACE = "wikidata"
from cassandra.query import dict_factory
cluster = Cluster(CASSANDRA_NODES)
session = cluster.connect(CASSANDRA_KEYSPACE)
session.row_factory = dict_factory

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

def get_time_range(hours: int, now: Optional[str]) -> tuple:
    if now:
        now_dt = isoparse(now).astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    else:
        now_dt = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    end_time = now_dt - timedelta(hours=1)
    start_time = end_time - timedelta(hours=hours)
    return start_time, end_time


def make_cache_key(prefix: str, start: datetime, end: datetime) -> str:
    raw = f"{prefix}:{start.isoformat()}_{end.isoformat()}"
    return raw

@app.get("/api/domains")
def get_domains():
    cached = redis_client.get("domains")
    if cached:
        return {"domains": json.loads(cached)}

    query = "SELECT domain FROM page_count_by_domain;"
    rows = session.execute(query)
    domains = sorted(set(row["domain"] for row in rows))

    redis_client.set("domains", json.dumps(domains), ex=3600)
    return {"domains": domains}


@app.get("/api/pages-by-user/{user_id}")
def get_pages_by_user(user_id: str):
    query = "SELECT page_id, page_title, domain, is_bot FROM pages_by_user WHERE user_id = %s;"
    rows = session.execute(query, (user_id,))
    pages = list(rows)
    return {"user_id": user_id, "pages": pages}

@app.get("/api/articles-count/{domain}")
def count_articles_for_domain(domain: str):
    try:
        query = "SELECT page_count FROM page_count_by_domain WHERE domain = %s;"
        row = session.execute(query, (domain,)).one()
        count = row["page_count"] if row and "page_count" in row else 0
        return {"domain": domain, "article_count": count}
    except Exception as e:
        return {"error": f"Internal Server Error: {str(e)}"}

@app.get("/api/page/{page_id}")
def get_page_by_id(page_id: str):
    query = "SELECT * FROM page_by_id WHERE page_id = %s;"
    row = session.execute(query, (page_id,)).one()
    if row:
        return row
    return {"error": "Page not found"}

@app.get("/api/active-users")
def get_active_users(
    hours: int = Query(..., description="Number of full hours to look back"),
    now: Optional[str] = Query(None, description="Optional ISO 8601 UTC time override")
):
    start_time, end_time = get_time_range(hours, now)
    cache_key = make_cache_key("active_users", start_time, end_time)

    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    result = []
    query = """
        SELECT user_id, user_name, page_count FROM user_activity
        WHERE time_window_start = %s;
    """

    for i in range(hours):
        hour = (start_time + timedelta(hours=i)).replace(tzinfo=timezone.utc)
        rows = session.execute(query, (hour,))
        for row in rows:
            result.append({
                "id": row["user_id"],
                "name": row["user_name"],
                "page_count": row["page_count"]
            })

    redis_client.set(cache_key, json.dumps(result), ex=3600)
    return result