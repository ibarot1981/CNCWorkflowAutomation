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
GRIST_TABLE_ID    = os.getenv("GRIST_TABLE_ID")     # Example: "CNCPartsMaster"
GRIST_API_URL     = os.getenv("GRIST_API_URL")      # Example: "http://localhost:8484/api"

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET")       # Example: "cnc-dxf"

if not all([GRIST_API_KEY, GRIST_DOC_ID, GRIST_TABLE_ID, GRIST_API_URL,
            MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET]):
    print("❌ Missing environment variables. Check your .env file.")
    sys.exit(1)

# -------------------------------------------------------
# LOGGING SETUP
# -------------------------------------------------------
LOGFILE = "upload_dxf_to_minio.log"

logger = logging.getLogger("dxf_uploader")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(LOGFILE, maxBytes=5_000_000, backupCount=5)
formatter = logging.Formatter('%(asctime)s — %(levelname)s — %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info("🚀 DXF Upload Script Started")

# -------------------------------------------------------
# MINIO CLIENT
# -------------------------------------------------------
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Ensure bucket exists
if not minio_client.bucket_exists(MINIO_BUCKET):
    logger.info(f"Bucket '{MINIO_BUCKET}' not found. Creating...")
    minio_client.make_bucket(MINIO_BUCKET)

# -------------------------------------------------------
# GRIST API HELPERS
# -------------------------------------------------------
headers = {"Authorization": f"Bearer {GRIST_API_KEY}"}

def grist_get_rows():
    url = f"{GRIST_API_URL}/docs/{GRIST_DOC_ID}/tables/{GRIST_TABLE_ID}/records"
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    return res.json()["records"]

def grist_update_row(row_id, fields: dict):
    url = f"{GRIST_API_URL}/docs/{GRIST_DOC_ID}/tables/{GRIST_TABLE_ID}/records/{row_id}"
    payload = {"fields": fields}
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
        logger.error(f"❌ MinIO upload failed for {local_path}: {e}")
        return False

# -------------------------------------------------------
# PROCESS CNC PART ENTRIES
# -------------------------------------------------------
def process_parts():
    logger.info("🔍 Fetching CNCPartsMaster rows from Grist...")

    rows = grist_get_rows()
    logger.info(f"📄 Retrieved {len(rows)} rows.")

    for r in rows:

        row_id = r["id"]
        fields = r["fields"]

        ready = fields.get("Ready")
        upload_flag = str(fields.get("Upload_to_Minio", "")).lower()
        filename = fields.get("DXF_Filename")
        folder = fields.get("FolderPath")

        # Skip conditions
        if ready != 1:
            continue
        if upload_flag not in ("yes", "y", "true"):
            continue
        if not filename or not folder:
            continue

        local_path = os.path.join(folder, filename)

        logger.info(f"➡ Processing row {row_id}: {filename}")

        # FILE CHECK
        if not os.path.isfile(local_path):
            logger.warning(f"⚠ File not found: {local_path}")
            grist_update_row(row_id, {
                "Upload_Status": "File Not Found",
                "UploadedOn": None
            })
            continue

        # MINIO PATH FORMAT:
        # cnc-parts/<prefix>/<filename>
        minio_path = f"DXF/{filename}"

        ok = upload_to_minio(local_path, minio_path)

        if ok:
            # Construct MinIO URL
            minio_url = f"http://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{minio_path}"
            logger.info(f"✔ Uploaded: {minio_url}")

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

        time.sleep(0.1)   # avoid hammering Grist


# -------------------------------------------------------
# MAIN
# -------------------------------------------------------
if __name__ == "__main__":
    try:
        process_parts()
        logger.info("🎉 DXF Upload Script Completed Successfully\n")
    except Exception as e:
        logger.exception("Fatal error in script.")
        print("\n❌ Fatal error — check log for details.\n")
