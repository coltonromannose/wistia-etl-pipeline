--- ## This is for setting up the schema tables in the redshift db 

CREATE TABLE IF NOT EXISTS public.fact_events (
  event_key                   VARCHAR(255) PRIMARY KEY,
  received_at                 TIMESTAMP,
  percent_viewed              FLOAT,
  embed_url                   VARCHAR(1024),
  email                       VARCHAR(255),
  ip                          VARCHAR(50),
  user_agent_browser          VARCHAR(100),
  user_agent_browser_version  VARCHAR(50),
  user_agent_platform         VARCHAR(100),
  user_agent_mobile           BOOLEAN,
  visitor_key                 VARCHAR(255),
  country                     VARCHAR(50),
  region                      VARCHAR(50),
  city                        VARCHAR(100),
  lat                         FLOAT,
  lon                         FLOAT,
  org                         VARCHAR(255),
  media_id                    VARCHAR(50),
  media_name                  VARCHAR(255)
);




CREATE TABLE IF NOT EXISTS public.dim_media (
    media_id VARCHAR PRIMARY KEY,
    media_name VARCHAR,
    duration_seconds FLOAT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    section_name VARCHAR,
    subfolder_name VARCHAR,
    thumbnail_url VARCHAR,
    project_name VARCHAR
);




CREATE TABLE IF NOT EXISTS public.media_daily_agg_stage (
    media_id        VARCHAR   NOT NULL,
    dt              DATE      NOT NULL,
    load_count      BIGINT,
    play_count      BIGINT,
    play_rate       DOUBLE PRECISION,
    hours_watched   DOUBLE PRECISION,
    engagement      DOUBLE PRECISION,
    visitors        BIGINT
);
