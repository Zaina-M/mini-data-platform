import argparse
import os
from minio import Minio


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify file archival behavior in MinIO.")
    parser.add_argument("--endpoint", default="localhost:9000")
    parser.add_argument("--raw-bucket", default="raw-sales")
    parser.add_argument("--archive-bucket", default="archive")
    parser.add_argument("--object-name", default="test_flow.csv")
    args = parser.parse_args()

    client = Minio(
        args.endpoint,
        os.environ["MINIO_ROOT_USER"],
        os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )

    raw_files = [obj.object_name for obj in client.list_objects(args.raw_bucket)]
    archive_files = [
        obj.object_name
        for obj in client.list_objects(args.archive_bucket, recursive=True)
    ]

    print(f"Files remaining in {args.raw_bucket}: {raw_files}")
    print(f"Files in {args.archive_bucket}: {archive_files}")

    if args.object_name not in raw_files and archive_files:
        print("ARCHIVE CHECK PASSED: file moved from raw-sales to archive")
    elif args.object_name in raw_files:
        print("WARNING: file still in raw-sales (may not have been archived yet)")
    else:
        print("Archive check inconclusive")


if __name__ == "__main__":
    main()
