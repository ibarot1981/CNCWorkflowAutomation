#!/usr/bin/env python3
import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from dotenv import load_dotenv

from minio import Minio
import requests

# -------------------------------------------------------
# LOAD ENVIRONMENT
# -------------------------------------------------------
load_dotenv()

GRIST_API_KEY     = os.getenv("GRIST_API_KEY")
GRIST_DOC_ID      = os.getenv("GRIST_DOC_ID")
GRIST_TABLE_ID    = os.getenv("GRIST_TABLE_ID")   # VERIFIED: CNCPartsMaster
GRIST_API_URL     = os.getenv("GRIST_API_URL")    # Must end with /api

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET")

required_env = [
    "GRIST_API_KEY", "GRIST_DOC_ID", "GRIST_TABLE_ID", "GRIST_API_URL",
    "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET"
]

for env in required_env:
    if not os.getenv(env):
        print(f"❌ Missing environment variable: {env}")
        sys.exit(1)

# -------------------------------------------------------
# LOGGING SETUP (UTF-8 SAFE)
# -------------------------------------------------------
LOGFILE = "upload_dxf_to_minio.log"

logger = logging.getLogger("dxf_uploader")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(
    LOGFILE, maxBytes=5_000_000, backupCount=5, encoding="utf-8"
)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

logger.info("DXF Upload Script Started")

# -------------------------------------------------------
# MINIO CLIENT
# -------------------------------------------------------
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not minio_client.bucket_exists(MINIO_BUCKET):
    logger.info(f"Bucket '{MINIO_BUCKET}' not found. Creating...")
    minio_client.make_bucket(MINIO_BUCKET)

# -------------------------------------------------------
# MAKE BUCKET PUBLIC (AUTO-APPLY POLICY)
# -------------------------------------------------------
import json

public_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{MINIO_BUCKET}/*"]
        }
    ]
}
try:
    minio_client.set_bucket_policy(MINIO_BUCKET, json.dumps(public_policy))
    logger.info(f"Applied public-read policy to bucket: {MINIO_BUCKET}")
except Exception as e:
    logger.error(f"Failed to apply bucket policy: {e}")

    
# -------------------------------------------------------
# GRIST API HELPERS
# -------------------------------------------------------
headers = {"Authorization": f"Bearer {GRIST_API_KEY}"}

def grist_get_rows():
    """Fetch all records from CNCPartsMaster."""
    url = f"{GRIST_API_URL}/docs/{GRIST_DOC_ID}/tables/{GRIST_TABLE_ID}/records"
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    return res.json()["records"]

def grist_update_row(row_id, fields: dict):
    """
    Correct PATCH format for Grist.
    Must send:
    {
      "records": [
         { "id": row_id, "fields": {...} }
      ]
    }
    """
    url = f"{GRIST_API_URL}/docs/{GRIST_DOC_ID}/tables/{GRIST_TABLE_ID}/records"
    payload = {
        "records": [
            {
                "id": row_id,
                "fields": fields
            }
        ]
    }
    res = requests.patch(url, json=payload, headers=headers)
    res.raise_for_status()

# -------------------------------------------------------
# MINIO UPLOAD HELPER
# -------------------------------------------------------
def upload_to_minio(local_path: str, minio_path: str):
    try:
        minio_client.fput_object(
            MINIO_BUCKET,
            minio_path,
            local_path
        )
        return True
    except Exception as e:
        logger.error(f"MinIO upload failed for {local_path}: {e}")
        return False

# -------------------------------------------------------
# MAIN WORKFLOW
# -------------------------------------------------------
def process_parts():
    logger.info("Fetching CNCPartsMaster rows from Grist...")

    rows = grist_get_rows()
    logger.info(f"Retrieved {len(rows)} rows.")

    for r in rows:
        row_id = r["id"]
        f = r["fields"]

        ready           = f.get("Ready")
        upload_flag     = str(f.get("Upload_to_Minio", "")).lower()
        upload_status   = (f.get("Upload_Status") or "").lower()
        filename        = f.get("DXF_Filename")
        folder          = f.get("FolderPath")
        thickness       = f.get("Thickness")

        # NEW: Read clean product prefix directly from Grist
        product_prefix = (f.get("CNCProductPrefix") or "").strip()

        # ----------------------------------------------------
        # SKIP CONDITIONS
        # ----------------------------------------------------
        if ready != 1:
            continue
        if upload_flag not in ("yes", "y", "true"):
            continue
        if upload_status == "success":
            continue
        if not filename or not folder:
            continue
        if not product_prefix:
            logger.error(f"EMPTY product prefix for row {row_id}")
            continue

        # Build local path
        local_path = os.path.join(folder, filename)


        logger.info(f"Processing row {row_id}: {filename}")

        if not os.path.isfile(local_path):
            logger.warning(f"File not found: {local_path}")
            grist_update_row(row_id, {
                "Upload_Status": "File Not Found",
                "UploadedOn": None
            })
            continue

        # Build MinIO path
        safe_product = product_prefix.replace("/", "_").replace("\\", "_")
        safe_product = safe_product.strip()

        minio_path = f"DXF/{safe_product}/{safe_thk}/{filename}"


        # Upload file
        ok = upload_to_minio(local_path, minio_path)

        if ok:
            minio_url = f"http://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{minio_path}"

            logger.info(f"Uploaded successfully: {minio_url}")

            grist_update_row(row_id, {
                "Upload_Status": "Success",
                "MinioPath": minio_url,
                "UploadedOn": datetime.now().isoformat()
            })

        else:
            grist_update_row(row_id, {
                "Upload_Status": "Failed",
                "UploadedOn": None
            })

        time.sleep(0.1)  # throttle

# -------------------------------------------------------
# SCRIPT ENTRYPOINT
# -------------------------------------------------------
if __name__ == "__main__":
    try:
        process_parts()
        logger.info("DXF Upload Script Completed Successfully")
    except Exception as e:
        logger.exception("Fatal error in script.")
        print("\n❌ Fatal error — check logs.\n")
