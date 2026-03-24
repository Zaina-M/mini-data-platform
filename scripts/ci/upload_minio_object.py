import argparse
import os
from minio import Minio


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload a file to MinIO and verify it exists.")
    parser.add_argument("--endpoint", default="localhost:9000")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--object-name", required=True)
    parser.add_argument("--file-path", required=True)
    args = parser.parse_args()

    client = Minio(
        args.endpoint,
        os.environ["MINIO_ROOT_USER"],
        os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )

    client.fput_object(args.bucket, args.object_name, args.file_path)
    objects = [o.object_name for o in client.list_objects(args.bucket)]

    print(f"Files in {args.bucket} after upload: {objects}")
    if args.object_name not in objects:
        raise RuntimeError(f"Upload verification failed for {args.object_name}")

    print(f"INGESTION CHECK PASSED: {args.object_name} uploaded to {args.bucket}")


if __name__ == "__main__":
    main()
