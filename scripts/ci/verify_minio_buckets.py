import argparse
import os
from minio import Minio


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify required MinIO buckets exist.")
    parser.add_argument("--endpoint", default="localhost:9000")
    parser.add_argument(
        "--required",
        nargs="+",
        default=["raw-sales", "processed", "archive"],
        help="Required bucket names",
    )
    args = parser.parse_args()

    client = Minio(
        args.endpoint,
        os.environ["MINIO_ROOT_USER"],
        os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )

    buckets = [bucket.name for bucket in client.list_buckets()]
    print(f"Buckets found: {buckets}")

    missing = [name for name in args.required if name not in buckets]
    if missing:
        raise RuntimeError(f"Missing bucket(s): {missing}")

    print("All required buckets exist")


if __name__ == "__main__":
    main()
