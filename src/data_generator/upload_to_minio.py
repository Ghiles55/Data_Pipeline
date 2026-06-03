"""
SPOTIFY — Upload des catalogues de labels vers MinIO
====================================================
Pousse les 3 fichiers JSON générés par generate_catalog.py
dans le bucket `labels-raw` de MinIO.

Usage (depuis la racine du repo, stack docker compose lancée) :
    pip install boto3
    python -m src.data_generator.upload_to_minio
"""

import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ENDPOINT_URL = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET = "labels-raw"
LABELS_DIR = Path("data/labels")


def main() -> int:
    files = sorted(LABELS_DIR.glob("*.json"))
    if not files:
        print(f"Aucun JSON trouvé dans {LABELS_DIR}/ — lance d'abord generate_catalog.py")
        return 1

    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    # Crée le bucket s'il n'existe pas (minio-init le crée déjà, mais on sécurise)
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Bucket '{BUCKET}' créé")

    for f in files:
        s3.upload_file(str(f), BUCKET, f.name)
        print(f"Uploadé : {f.name} → s3://{BUCKET}/{f.name}")

    # Vérification
    objs = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    print(f"\n{len(objs)} objets dans '{BUCKET}': {[o['Key'] for o in objs]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
