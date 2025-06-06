from fastapi import FastAPI
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from dateutil.parser import isoparse
import uvicorn
import redis.asyncio as redis
import json
import hashlib
import asyncio

app = FastAPI()

CASSANDRA_NODES = ["cassandra"]
KEYSPACE = "wikidata"
cluster = Cluster(CASSANDRA_NODES)
session = cluster.connect(KEYSPACE)
session.row_factory = dict_factory

redis_client = redis.Redis(host="redis", port=6379, db=0)

def make_cache_key(prefix: str, start: datetime, end: datetime):
    raw = f"{prefix}:{start.isoformat()}_{end.isoformat()}"
    return hashlib.md5(raw.encode()).hexdigest()

def get_time_range():
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    end = now - timedelta(hours=1)
    start = end - timedelta(hours=6)
    return start, end

@app.get("/api/pages-by-hour")
async def pages_by_hour():
    start_time, end_time = get_time_range()
    cache_key = make_cache_key("pages_by_hour", start_time, end_time)

    if cached := await redis_client.get(cache_key):
        return json.loads(cached)

    response = []
    query = """
        SELECT domain, page_count
        FROM pages_per_domain_by_hour
        WHERE hour_window_start = %s;
    """

    for i in range(6):
        hour_start = (start_time + timedelta(hours=i)).replace(tzinfo=timezone.utc)
        hour_end = hour_start + timedelta(hours=1)

        rows = session.execute(query, (hour_start,))
        stats = [{row["domain"]: row["page_count"]} for row in rows]

        response.append({
            "time_start": hour_start.strftime("%H:%M"),
            "time_end": hour_end.strftime("%H:%M"),
            "statistics": stats
        })

    await redis_client.setex(cache_key, 60, json.dumps(response))
    return response


@app.get("/api/bot-pages-by-domain")
async def bot_pages_by_domain():
    start_time, end_time = get_time_range()
    cache_key = make_cache_key("bot_pages_by_domain", start_time, end_time)

    if cached := await redis_client.get(cache_key):
        return json.loads(cached)

    domain_rows = session.execute("SELECT domain FROM domains;")
    domains = [row["domain"] for row in domain_rows]

    results = defaultdict(int)
    query = """
        SELECT pages_by_bots
        FROM bot_pages_per_domain_summary
        WHERE time_window_start = %s AND domain = %s;
    """

    for i in range(6):
        hour_start = (start_time + timedelta(hours=i)).replace(tzinfo=timezone.utc)
        for domain in domains:
            rows = session.execute(query, (hour_start, domain))
            total = sum(row["pages_by_bots"] for row in rows)
            if total > 0:
                results[domain] += total

    response = {
        "time_start": start_time.strftime("%H:%M"),
        "time_end": end_time.strftime("%H:%M"),
        "statistics": [
            {"domain": domain, "created_by_bots": count}
            for domain, count in results.items()
            if count > 0
        ]
    }

    await redis_client.setex(cache_key, 60, json.dumps(response))
    return response

@app.get("/api/top-users")
async def top_users():
    start_time, end_time = get_time_range()
    cache_key = make_cache_key("top_users", start_time, end_time)

    if cached := await redis_client.get(cache_key):
        return json.loads(cached)

    query = """
        SELECT user_id, user_name, page_count
        FROM top_users_page_creation
        WHERE time_window_start = %s;
    """

    user_stats = defaultdict(lambda: {"pages_created": 0, "user_name": ""})

    for i in range(6):
        hour = (start_time + timedelta(hours=i)).replace(minute=0, second=0, microsecond=0, tzinfo=None)
        rows = session.execute(query, (hour,))
        for row in rows:
            user_id = row["user_id"]
            user_stats[user_id]["pages_created"] += row["page_count"]
            user_stats[user_id]["user_name"] = row["user_name"]

    data = []
    for user_id, info in user_stats.items():
        data.append({
            "user_id": user_id,
            "user_name": info["user_name"],
            "pages_created": info["pages_created"],
            "time_start": start_time.strftime("%H:%M"),
            "time_end": end_time.strftime("%H:%M")
        })

    top_20 = sorted(data, key=lambda x: x["pages_created"], reverse=True)[:20]
    await redis_client.setex(cache_key, 60, json.dumps(top_20))
    return top_20

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)