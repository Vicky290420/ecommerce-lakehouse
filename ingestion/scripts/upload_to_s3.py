import boto3
import os
import yaml
import logging
from datetime import datetime
from botocore.exceptions import ClientError

# ── Logging setup ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def get_s3_client(region):
    """Creates and returns an S3 client using your AWS CLI credentials"""
    return boto3.client('s3', region_name=region)


def upload_file(s3_client, local_path, bucket, s3_key):
    """
    Uploads a single file to S3.
    Returns True if successful, False if failed.
    """
    try:
        file_size = os.path.getsize(local_path)
        logger.info(f"Uploading {os.path.basename(local_path)} ({file_size:,} bytes)...")

        s3_client.upload_file(local_path, bucket, s3_key)

        logger.info(f"✅ Uploaded → s3://{bucket}/{s3_key}")
        return True

    except ClientError as e:
        logger.error(f"❌ Failed to upload {local_path}: {e}")
        return False

    except FileNotFoundError:
        logger.error(f"❌ File not found: {local_path}")
        return False


def main():
    config = load_config()
    aws_cfg = config['aws']

    bucket      = aws_cfg['bucket_name']
    region      = aws_cfg['region']
    raw_prefix  = aws_cfg['raw_prefix']

    # Batch ID — this is the key pattern for idempotency
    # Every run gets its own folder so nothing ever overwrites
    batch_id = datetime.now().strftime('%Y-%m-%d_%H-%M')
    logger.info(f"Starting upload | Batch ID: {batch_id}")

    s3_client = get_s3_client(region)

    # Files to upload
    data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'sample')

    files = {
        'customers.csv': f"{raw_prefix}customers/batch_id={batch_id}/customers.csv",
        'products.csv':  f"{raw_prefix}products/batch_id={batch_id}/products.csv",
        'orders.csv':    f"{raw_prefix}orders/batch_id={batch_id}/orders.csv",
    }

    # Upload each file
    results = {}
    for filename, s3_key in files.items():
        local_path = os.path.join(data_dir, filename)
        results[filename] = upload_file(s3_client, local_path, bucket, s3_key)

    # Summary
    success = sum(results.values())
    total   = len(results)

    print(f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Upload Summary
  Batch ID : {batch_id}
  Success  : {success}/{total} files
  Bucket   : s3://{bucket}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    """)

    if success < total:
        logger.error("Some files failed to upload. Check logs above.")
        exit(1)
    else:
        logger.info("All files uploaded successfully. Bronze layer is ready.")


if __name__ == '__main__':
    main()