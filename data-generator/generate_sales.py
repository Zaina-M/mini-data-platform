"""
Sales Data Generator

Generates realistic sales data with controlled data quality issues
for testing the data pipeline.
"""

import argparse
import csv
import os
import random
from datetime import datetime, timedelta
from io import StringIO


class SalesDataGenerator:
    """Generate synthetic sales data with configurable quality issues."""

    PRODUCTS = [
        ("Laptop Pro 15", 999.99),
        ("Wireless Mouse", 29.99),
        ("USB-C Hub", 49.99),
        ("Mechanical Keyboard", 129.99),
        ("Monitor 27inch", 349.99),
        ("Webcam HD", 79.99),
        ("Headphones BT", 159.99),
        ("External SSD 1TB", 109.99),
        ("Tablet Stand", 39.99),
        ("Power Bank 20000", 45.99),
        ("Smart Watch", 249.99),
        ("Bluetooth Speaker", 89.99),
    ]

    COUNTRIES = [
        "United States",
        "Canada",
        "United Kingdom",
        "Germany",
        "France",
        "Australia",
        "Japan",
        "Brazil",
        "India",
        "Mexico",
        "Spain",
        "Italy",
        "Netherlands",
        "Sweden",
        "Singapore",
    ]

    def __init__(self, num_records=100, error_rate=0.05):
        """
        Initialize generator.

        Args:
            num_records: Number of records to generate
            error_rate: Proportion of records with data quality issues (0-1)
        """
        self.num_records = num_records
        self.error_rate = error_rate
        self.order_counter = 1000

    def generate_order_id(self):
        """Generate unique order ID."""
        self.order_counter += 1
        return f"ORD-{self.order_counter:06d}"

    def generate_customer_id(self):
        """Generate customer ID."""
        return f"CUST-{random.randint(10000, 99999)}"

    def generate_date(self, days_back=90):
        """Generate random date within specified days back."""
        start_date = datetime.now() - timedelta(days=days_back)
        random_days = random.randint(0, days_back)
        return start_date + timedelta(days=random_days)

    def introduce_error(self, record):
        """Introduce a random data quality issue."""
        error_type = random.choice(
            [
                "null_quantity",
                "negative_quantity",
                "invalid_date",
                "null_customer",
                "whitespace_product",
                "zero_price",
                "duplicate_spaces",
                "empty_country",
            ]
        )

        if error_type == "null_quantity":
            record["quantity"] = ""
        elif error_type == "negative_quantity":
            record["quantity"] = -random.randint(1, 10)
        elif error_type == "invalid_date":
            record["order_date"] = "invalid-date"
        elif error_type == "null_customer":
            record["customer_id"] = ""
        elif error_type == "whitespace_product":
            record["product_name"] = f"  {record['product_name']}  "
        elif error_type == "zero_price":
            record["unit_price"] = 0
        elif error_type == "duplicate_spaces":
            record["country"] = record["country"].replace(" ", "  ")
        elif error_type == "empty_country":
            record["country"] = ""

        return record

    def generate_record(self):
        """Generate a single sales record."""
        product = random.choice(self.PRODUCTS)
        order_date = self.generate_date()

        record = {
            "order_id": self.generate_order_id(),
            "product_name": product[0],
            "quantity": random.randint(1, 10),
            "unit_price": product[1],
            "order_date": order_date.strftime("%Y-%m-%d"),
            "customer_id": self.generate_customer_id(),
            "country": random.choice(self.COUNTRIES),
        }

        # Introduce errors based on error_rate
        if random.random() < self.error_rate:
            record = self.introduce_error(record)

        return record

    def generate_dataset(self):
        """Generate complete dataset."""
        records = []
        for _ in range(self.num_records):
            records.append(self.generate_record())

        # Occasionally add duplicate order_ids
        if self.error_rate > 0 and len(records) > 10:
            num_duplicates = int(len(records) * self.error_rate * 0.5)
            for _ in range(num_duplicates):
                idx = random.randint(0, len(records) - 1)
                duplicate = records[idx].copy()
                duplicate["quantity"] = random.randint(1, 5)
                records.append(duplicate)

        return records

    def to_csv(self, records):
        """Convert records to CSV string."""
        output = StringIO()
        fieldnames = [
            "order_id",
            "product_name",
            "quantity",
            "unit_price",
            "order_date",
            "customer_id",
            "country",
        ]

        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

        return output.getvalue()

    def save_to_file(self, filename, records=None):
        """Save dataset to CSV file."""
        if records is None:
            records = self.generate_dataset()

        csv_content = self.to_csv(records)

        with open(filename, "w", newline="", encoding="utf-8") as f:
            f.write(csv_content)

        return filename


def upload_to_minio(file_path, bucket_name="raw-sales"):
    """Upload CSV file to MinIO."""
    from minio import Minio

    client = Minio(
        os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )

    # Ensure bucket exists
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    file_name = os.path.basename(file_path)
    client.fput_object(bucket_name, file_name, file_path)

    print(f"Uploaded {file_name} to MinIO bucket '{bucket_name}'")
    return file_name


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate sales data for testing")
    parser.add_argument("--records", type=int, default=100, help="Number of records to generate")
    parser.add_argument(
        "--error-rate", type=float, default=0.05, help="Rate of data quality issues (0-1)"
    )
    parser.add_argument("--output", type=str, default=None, help="Output file path")
    parser.add_argument("--upload", action="store_true", help="Upload to MinIO after generation")

    args = parser.parse_args()

    generator = SalesDataGenerator(num_records=args.records, error_rate=args.error_rate)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Default output to data/output folder (relative to project root)
    if args.output:
        filename = args.output
    else:
        output_dir = os.path.join(os.path.dirname(__file__), "..", "data", "output")
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, f"sales_data_{timestamp}.csv")

    records = generator.generate_dataset()
    generator.save_to_file(filename, records)

    clean_records = len([r for r in records if all(r.values())])
    print(f"Generated {len(records)} records ({clean_records} clean)")
    print(f"Saved to: {filename}")

    if args.upload:
        try:
            upload_to_minio(filename)
        except Exception as e:
            print(f"Upload failed: {e}")
            print("Make sure MinIO is running and accessible")


if __name__ == "__main__":
    main()
