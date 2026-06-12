-- Runs automatically when you start: docker compose run --rm duckdb
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;

-- Credentials for reading/writing data files on MinIO
CREATE OR REPLACE SECRET minio (
    TYPE s3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'minio:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);

-- The Iceberg catalog (REST facade in front of the Hive Metastore).
-- Empty warehouse string = the facade's default (and only) catalog.
ATTACH '' AS lake (
    TYPE iceberg,
    ENDPOINT 'http://iceberg-rest:9001/iceberg',
    AUTHORIZATION_TYPE 'none'
);

SELECT 'catalog attached, tables:' AS info;
SHOW ALL TABLES;
