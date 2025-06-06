## Design

- Kafka – to collect events in real time from Wikimedia stream.

- Spark Structured Streaming – to parse and aggregate data from Kafka efficiently.

- Cassandra – as a distributed, high-throughput database for storing all real-time and aggregated metrics.

- FastAPI – for querying processed data from Cassandra.

## Diagrams

Cassandra tables

```sql
CREATE TABLE IF NOT EXISTS wikidata.page_count_by_domain (
    domain TEXT PRIMARY KEY,
    page_count INT
);

Simply domains + number of created articles per domain


CREATE TABLE IF NOT EXISTS wikidata.pages_by_user (
    user_id TEXT,
    page_id TEXT,
    page_title TEXT,
    domain TEXT,
    is_bot BOOLEAN,
    PRIMARY KEY (user_id, page_id)
);

Table to get pages by user_id, so partition is user_id


CREATE TABLE IF NOT EXISTS wikidata.page_by_id (
    page_id TEXT PRIMARY KEY,
    domain TEXT,
    page_title TEXT
);

Get page by id, so simple primary is page_id


CREATE TABLE IF NOT EXISTS wikidata.user_activity (
    time_window_start TIMESTAMP,
    time_window_end TIMESTAMP,
    user_id TEXT,
    user_name TEXT,
    page_count INT,
    PRIMARY KEY (time_window_start, user_id)
);

Table for this query: Return the id, name, and the number of created pages of all the users who created at least one page in a specified time range


CREATE TABLE IF NOT EXISTS wikidata.top_users_page_creation (
    time_window_start TIMESTAMP,
    time_window_end TIMESTAMP,
    user_id TEXT,
    user_name TEXT,
    page_count INT,
    PRIMARY KEY (time_window_start, user_id)
);

Table to get top users by page count in each hour


CREATE TABLE IF NOT EXISTS wikidata.bot_pages_per_domain_summary (
    domain TEXT,
    time_window_start TIMESTAMP,
    time_window_end TIMESTAMP,
    pages_by_bots INT,
    PRIMARY KEY (time_window_start, domain)
);

Bot stats by domain for each hour


CREATE TABLE IF NOT EXISTS wikidata.pages_per_domain_by_hour (
    hour_window_start TIMESTAMP,
    domain TEXT,
    page_count INT,
    PRIMARY KEY (hour_window_start, domain)
);

```


Інструкція з використання
Запуск усіх сервісів Kafka

<pre lang="bash">./run-kafka.sh</pre>
Перевірка запущених контейнерів

<pre lang="bash">docker ps</pre>
Створення топіку для читання і запису

<pre lang="bash">./create-topics.sh</pre>
Запуск Cassandra у Docker

<pre lang="bash">./run-cassandra-cluster.sh</pre>
Ініціалізація таблиць у Cassandra

<pre lang="bash">./init-cassandra.sh</pre>
Запуск генератора подій Wikipedia

<pre lang="bash">./run-generator.sh</pre>
Запуск усіх контейнерів через docker-compose

<pre lang="bash">./run.sh</pre>
Вхід у Spark-контейнер

<pre lang="bash">./run-container.sh</pre>
Перехід у потрібну папку всередині контейнера

<pre lang="bash">cd /opt/app</pre>

Запуск Spark Streaming, який читає з Kafka

<pre lang="bash">spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 spark_process.py</pre>
Перевірка, чи Kafka обробляє повідомлення (у новому терміналі)

<pre lang="bash">docker run -it --rm \ --network test-network \ -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 \ bitnami/kafka:3.3.2 kafka-console-consumer.sh \ --bootstrap-server kafka-server:9092 \ --topic processed \ --from-beginning</pre>
Запис агрегованих даних у Cassandra зі Spark

<pre lang="bash">spark-submit \ --conf spark.jars.ivy=/opt/src/a \ --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0" \ --master spark://spark:7077 \ --deploy-mode client \ spark_to_cassandra.py</pre>
Перевірка запису у Cassandra

<pre lang="bash">docker exec -it cassandra cqlsh</pre> <pre lang="bash">USE wikidata;</pre> <pre lang="bash">SELECT * FROM pages_by_user LIMIT 10;</pre>

Запуск апішок
<pre lang="bash">cd api1</pre>
<pre lang="bash">./run-api1</pre>

<pre lang="bash">cd api2</pre>
<pre lang="bash">./run-api2</pre>


##API Examples
1. /api/pages-by-hour
Returns: list of domains and number of created pages per hour (for last 6 hours)
```plaintext
[
  {
    "time_start": "06:00",
    "time_end": "07:00",
    "statistics": [
      {"commons.wikimedia.org": 318},
      {"en.wikipedia.org": 48},
      {"www.wikidata.org": 36},
      {"tr.wiktionary.org": 12},
      {"es.wikipedia.org": 21}
      // ...
    ]
  },
  {
    "time_start": "07:00",
    "time_end": "08:00",
    "statistics": [
      {"commons.wikimedia.org": 420},
      {"en.wikipedia.org": 37},
      {"www.wikidata.org": 79},
      {"ru.wikipedia.org": 24}
      // ...
    ]
  }
]

```
2. /api/bot-pages-by-domain
Returns: total number of pages created by bots per domain in the last 6 hours
```plaintext
{
  "time_start": "03:00",
  "time_end": "09:00",
  "statistics": [
    {"domain": "commons.wikimedia.org", "created_by_bots": 1034},
    {"domain": "www.wikidata.org", "created_by_bots": 17},
    {"domain": "zgh.wikipedia.org", "created_by_bots": 58},
    {"domain": "auth.wikimedia.org", "created_by_bots": 7},
    {"domain": "ru.wikipedia.org", "created_by_bots": 7}
    // ...
  ]
}

```

3. /api/top-users
Returns: top 20 users by number of created pages over the last 6 hours
```plaintext
[
  {
    "user_id": "47722236",
    "user_name": "PEPSI697",
    "pages_created": 6,
    "time_start": "03:00",
    "time_end": "09:00"
  }..
]

```


1. /api/domains
Returns: all distinct domains from which pages were created.
```plaintext
{
  "domains": [
    "ab.wikipedia.org",
    "ar.wikipedia.org",
    "commons.wikimedia.org",
    "en.wikipedia.org",
    "fr.wikipedia.org",
    "www.wikidata.org"
    // ...
  ]
}

```

2. /api/pages-by-user/{user_id}
Returns: all pages created by a specific user.
```plaintext
{
  "user_id": "1615",
  "pages": [
    {
      "page_id": "975874",
      "page_title": "Pagina:Villani.djvu/19",
      "domain": "it.wikisource.org",
      "is_bot": false
    },
    {
      "page_id": "975905",
      "page_title": "Pagina:Villani.djvu/32",
      "domain": "it.wikisource.org",
      "is_bot": false
    }
  ]
}


```

3. /api/articles-count/{domain}
Returns: number of pages created for the specified domain.
```plaintext
{
  "domain": "ja.wikipedia.org",
  "article_count": 45
}

```
4. /api/page/{page_id}
Returns: information about a specific page by its ID
```plaintext
{
  "page_id": "165515326",
  "domain": "commons.wikimedia.org",
  "page_title": "File:Ermita_de_San_José.jpg"
}

```

5. /api/page/{page_id}
Returns: information about a specific page by its ID
```plaintext
[
  { "id": "910182", "name": "GeographBot", "page_count": 45 },
  { "id": "6018", "name": "ZioNicco", "page_count": 3 },
  { "id": "33641", "name": "Tuvok1968", "page_count": 1 }
]



```