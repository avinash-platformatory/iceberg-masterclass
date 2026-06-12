"""Load Polaris REST catalog using credentials from polaris-setup."""

from __future__ import annotations

import os
from pathlib import Path

from pyiceberg.catalog import load_catalog

CREDENTIALS_FILE = Path("/polaris-config/credentials.env")


def _load_credentials() -> dict[str, str]:
    if CREDENTIALS_FILE.is_file():
        creds: dict[str, str] = {}
        for line in CREDENTIALS_FILE.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            creds[key] = value
        return creds
    return {}


def load_polaris_catalog():
    file_creds = _load_credentials()

    polaris_url = os.environ.get("POLARIS_URL", file_creds.get("POLARIS_URL", "http://polaris:8181"))
    catalog_name = os.environ.get(
        "POLARIS_CATALOG_NAME", file_creds.get("POLARIS_CATALOG_NAME", "lake")
    )
    client_id = os.environ.get("POLARIS_USER_CLIENT_ID", file_creds.get("POLARIS_USER_CLIENT_ID", ""))
    client_secret = os.environ.get(
        "POLARIS_USER_CLIENT_SECRET", file_creds.get("POLARIS_USER_CLIENT_SECRET", "")
    )
    if not client_id or not client_secret:
        raise RuntimeError(
            "Missing Polaris credentials — run: docker compose run --rm polaris-setup"
        )

    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    return load_catalog(
        "polaris",
        **{
            "type": "rest",
            "uri": f"{polaris_url}/api/catalog",
            "oauth2-server-uri": f"{polaris_url}/api/catalog/v1/oauth/tokens",
            "warehouse": catalog_name,
            "credential": f"{client_id}:{client_secret}",
            "scope": "PRINCIPAL_ROLE:ALL",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3.endpoint": minio_endpoint,
            "s3.access-key-id": minio_key,
            "s3.secret-access-key": minio_secret,
            "client.region": "us-east-1",
        },
    )
