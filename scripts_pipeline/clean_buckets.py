#!/usr/bin/env python3
"""Clean all MinIO buckets (bronze, silver, gold)"""
from minio import Minio
from env_config import get_minio_config

minio_config = get_minio_config()

client = Minio(
    minio_config["endpoint"],
    access_key=minio_config["access_key"],
    secret_key=minio_config["secret_key"],
    secure=False
)

print("\nCleaning MinIO buckets...\n")

for bucket in ['bronze', 'silver', 'gold']:
    try:
        objects = list(client.list_objects(bucket, recursive=True))
        count = 0
        for obj in objects:
            client.remove_object(bucket, obj.object_name)
            count += 1
        print(f"[OK] {bucket}: Deleted {count} files")
    except Exception as e:
        print(f"[ERROR] {bucket}: {e}")

print("\nBuckets cleaned!")
