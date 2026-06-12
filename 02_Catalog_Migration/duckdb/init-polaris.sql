-- Runs when you start: docker compose run --rm duckdb
-- Prerequisite: polaris-setup (credentials in /polaris-config/credentials.env)
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;

CREATE OR REPLACE SECRET minio (
    TYPE s3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'minio:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);

-- Catalog A: Gravitino REST facade over HMS (module 01)
ATTACH '' AS lake (
    TYPE iceberg,
    ENDPOINT 'http://iceberg-rest:9001/iceberg',
    AUTHORIZATION_TYPE 'none'
);

SELECT 'Catalog A (lake / Gravitino) attached.' AS info;
SHOW ALL TABLES;
