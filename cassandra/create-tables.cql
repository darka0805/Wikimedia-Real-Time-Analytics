CREATE KEYSPACE IF NOT EXISTS wikidata 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};


CREATE TABLE IF NOT EXISTS wikidata.pages (
  user_id text,
  domain text,
  created_at text,
  page_title text,
  PRIMARY KEY (user_id, created_at)
);

CREATE TABLE IF NOT EXISTS wikidata.try (
  user_id text,
  user_name text,
  domain text,
  created_at text,
  created_hour text,
  page_title text,
  is_bot boolean,
  page_id text,
  PRIMARY KEY (user_id, created_at)
);


CREATE TABLE IF NOT EXISTS wikidata.pages_per_domain_by_hour (
    hour_window_start TIMESTAMP,
    domain TEXT,
    page_count INT,
    PRIMARY KEY (hour_window_start, domain)
) WITH CLUSTERING ORDER BY (domain ASC);


CREATE TABLE IF NOT EXISTS wikidata.bot_pages_per_domain_summary (
    domain TEXT,
    time_window_start TIMESTAMP,
    time_window_end TIMESTAMP,
    pages_by_bots INT,
    PRIMARY KEY (time_window_start, domain)
);

CREATE TABLE IF NOT EXISTS wikidata.top_users_page_creation (
    time_window_start TIMESTAMP,
    time_window_end TIMESTAMP,
    user_id TEXT,
    user_name TEXT,
    page_count INT,
    PRIMARY KEY (time_window_start, user_id)
) WITH CLUSTERING ORDER BY (user_id ASC);



CREATE TABLE IF NOT EXISTS wikidata.domains (
    domain TEXT PRIMARY KEY,
    created_at TIMESTAMP,
    page_id TEXT
);

CREATE TABLE IF NOT EXISTS wikidata.pages_by_user (
    user_id TEXT,
    page_id TEXT,
    page_title TEXT,
    domain TEXT,
    is_bot BOOLEAN,
    PRIMARY KEY (user_id, page_id)
);

CREATE TABLE IF NOT EXISTS wikidata.page_count_by_domain (
    domain TEXT PRIMARY KEY,
    page_count INT,
);

CREATE TABLE IF NOT EXISTS wikidata.page_by_id (
    page_id TEXT PRIMARY KEY,
    domain TEXT,
    page_title TEXT
);

CREATE TABLE IF NOT EXISTS wikidata.user_activity (
    time_window_start TIMESTAMP,
    time_window_end TIMESTAMP,
    user_id TEXT,
    user_name TEXT,
    page_count INT,
    PRIMARY KEY (time_window_start, user_id)
);
