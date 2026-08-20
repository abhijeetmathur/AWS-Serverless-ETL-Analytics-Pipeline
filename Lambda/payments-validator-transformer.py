import json
import csv
import io
import os
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

S3_BUCKET = "payments-analytics-2026"

RAW_PREFIX = "raw/"
PROCESSED_PREFIX = "processed/"
REJECTED_PREFIX = "rejected/"

REQUIRED_COLUMNS = [
    "transaction_id",
    "transaction_ts",
    "merchant_id",
    "customer_id",
    "payment_method_id",
    "amount",
    "currency",
    "status",
    "is_fraud_flag",
    "device_type",
    "region"
]

VALID_STATUSES = {
    "SUCCESS",
    "FAILED",
    "DECLINED",
    "REFUNDED",
    "PENDING"
}

VALID_CURRENCIES = {
    "INR",
    "USD"
}

MIN_AMOUNT = Decimal("0.01")
MAX_AMOUNT = Decimal("10000000.00")


# ---------------------------------------------------------
# AWS clients
# ---------------------------------------------------------

s3 = boto3.client("s3")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ---------------------------------------------------------
# Timestamp parser
# ---------------------------------------------------------

def parse_timestamp(value):
    """
    Convert supported timestamp formats into a Python datetime.
    """

    if not value:
        raise ValueError("EMPTY_TIMESTAMP")

    value = value.strip()

    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%m/%d/%Y %I:%M %p",
        "%Y/%m/%d"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    raise ValueError("INVALID_TIMESTAMP")


# ---------------------------------------------------------
# Amount validation
# ---------------------------------------------------------

def parse_amount(value):

    if value is None or str(value).strip() == "":
        raise ValueError("EMPTY_AMOUNT")

    try:
        amount = Decimal(str(value))
    except InvalidOperation:
        raise ValueError("INVALID_AMOUNT")

    if amount < MIN_AMOUNT:
        raise ValueError("AMOUNT_OUT_OF_RANGE")

    if amount > MAX_AMOUNT:
        raise ValueError("AMOUNT_OUT_OF_RANGE")

    return amount


# ---------------------------------------------------------
# S3 helper
# ---------------------------------------------------------

def read_s3_object(bucket, key):

    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    return response["Body"].read()


# ---------------------------------------------------------
# Write rejected records
# ---------------------------------------------------------

def write_rejected_records(
    rejected_records,
    source_file
):

    if not rejected_records:
        return None

    output = io.StringIO()

    fieldnames = REQUIRED_COLUMNS + ["rejection_reason"]

    writer = csv.DictWriter(
        output,
        fieldnames=fieldnames
    )

    writer.writeheader()

    for record in rejected_records:
        writer.writerow(record)

    filename = os.path.basename(source_file)

    rejected_key = (
        REJECTED_PREFIX
        + filename.replace(".csv", "_rejected.csv")
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=rejected_key,
        Body=output.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

    logger.info(
        "Rejected records written to s3://%s/%s",
        S3_BUCKET,
        rejected_key
    )

    return rejected_key


# ---------------------------------------------------------
# Write Parquet
# ---------------------------------------------------------

def write_parquet(
    valid_records,
    source_file
):

    if not valid_records:
        return None

    table = pa.Table.from_pylist(valid_records)

    source_filename = os.path.basename(source_file)

    # Extract date from transaction timestamp
    first_timestamp = valid_records[0]["transaction_ts"]

    date_obj = datetime.fromisoformat(
        first_timestamp
    )

    year = date_obj.year
    month = date_obj.month
    day = date_obj.day

    output_buffer = io.BytesIO()

    pq.write_table(
        table,
        output_buffer,
        compression="snappy"
    )

    processed_key = (
        f"{PROCESSED_PREFIX}"
        f"year={year}/"
        f"month={month:02d}/"
        f"day={day:02d}/"
        f"{source_filename.replace('.csv', '.parquet')}"
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=processed_key,
        Body=output_buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    logger.info(
        "Processed Parquet written to s3://%s/%s",
        S3_BUCKET,
        processed_key
    )

    return processed_key


# ---------------------------------------------------------
# Main Lambda
# ---------------------------------------------------------

def lambda_handler(event, context):

    logger.info(
        "Received event: %s",
        json.dumps(event)
    )

    # -----------------------------------------------------
    # Extract S3 event information
    # -----------------------------------------------------

    try:

        # =================================================
        # EventBridge S3 event
        # =================================================

        if (
            event.get("source") == "aws.s3"
            and event.get("detail-type") == "Object Created"
        ):

            bucket = event["detail"]["bucket"]["name"]

            key = event["detail"]["object"]["key"]

        # =================================================
        # Direct S3 notification event
        # =================================================

        elif "Records" in event:

            record = event["Records"][0]

            bucket = record["s3"]["bucket"]["name"]

            key = record["s3"]["object"]["key"]

        # =================================================
        # Unsupported event
        # =================================================

        else:

            raise ValueError(
                "UNSUPPORTED_EVENT_FORMAT"
            )

        # S3 event keys may contain URL encoding

        from urllib.parse import unquote_plus

        key = unquote_plus(key)

        logger.info(
            "Processing S3 object: s3://%s/%s",
            bucket,
            key
        )

    except Exception as e:

        logger.exception(
            "Unable to parse S3 event"
        )

        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "INVALID_S3_EVENT",
                "message": str(e)
            })
        }

    # -----------------------------------------------------
    # Ignore non-CSV files
    # -----------------------------------------------------

    if not key.lower().endswith(".csv"):

        logger.info(
            "Ignoring non-CSV file: %s",
            key
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Ignored non-CSV file",
                "key": key
            })
        }

    # -----------------------------------------------------
    # Read source file
    # -----------------------------------------------------

    file_bytes = read_s3_object(
        bucket,
        key
    )

    text = file_bytes.decode(
        "utf-8-sig"
    )

    reader = csv.DictReader(
        io.StringIO(text)
    )

    # -----------------------------------------------------
    # Validate schema
    # -----------------------------------------------------

    actual_columns = reader.fieldnames or []

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in actual_columns
    ]

    if missing_columns:

        logger.error(
            "Missing required columns: %s",
            missing_columns
        )

        raise ValueError(
            f"MISSING_REQUIRED_COLUMNS: {missing_columns}"
        )

    # -----------------------------------------------------
    # Process records
    # -----------------------------------------------------

    valid_records = []

    rejected_records = []

    seen_transaction_ids = set()

    for row_number, row in enumerate(
        reader,
        start=2
    ):

        rejection_reason = None

        transaction_id = (
            row.get("transaction_id") or ""
        ).strip()

        # -----------------------------------------------
        # transaction_id
        # -----------------------------------------------

        if not transaction_id:

            rejection_reason = "MISSING_TRANSACTION_ID"

        elif transaction_id in seen_transaction_ids:

            rejection_reason = "DUPLICATE_TRANSACTION_ID"

        # -----------------------------------------------
        # Timestamp
        # -----------------------------------------------

        normalized_timestamp = None

        if rejection_reason is None:

            try:

                timestamp = parse_timestamp(
                    row.get("transaction_ts")
                )

                normalized_timestamp = (
                    timestamp.isoformat()
                )

            except ValueError as e:

                rejection_reason = str(e)

        # -----------------------------------------------
        # Amount
        # -----------------------------------------------

        amount = None

        if rejection_reason is None:

            try:

                amount = parse_amount(
                    row.get("amount")
                )

            except ValueError as e:

                rejection_reason = str(e)

        # -----------------------------------------------
        # Currency
        # -----------------------------------------------

        currency = (
            row.get("currency") or ""
        ).strip().upper()

        if (
            rejection_reason is None
            and currency not in VALID_CURRENCIES
        ):

            rejection_reason = "INVALID_CURRENCY"

        # -----------------------------------------------
        # Status
        # -----------------------------------------------

        status = (
            row.get("status") or ""
        ).strip().upper()

        if (
            rejection_reason is None
            and status not in VALID_STATUSES
        ):

            rejection_reason = "INVALID_STATUS"

        # -----------------------------------------------
        # Merchant
        # -----------------------------------------------

        merchant_id = (
            row.get("merchant_id") or ""
        ).strip()

        if (
            rejection_reason is None
            and not merchant_id
        ):

            rejection_reason = "MISSING_MERCHANT_ID"

        # -----------------------------------------------
        # Payment method
        # -----------------------------------------------

        payment_method_id = (
            row.get("payment_method_id") or ""
        ).strip()

        if (
            rejection_reason is None
            and not payment_method_id
        ):

            rejection_reason = "MISSING_PAYMENT_METHOD_ID"

        # -----------------------------------------------
        # Region
        # -----------------------------------------------

        region = (
            row.get("region") or ""
        ).strip()

        if (
            rejection_reason is None
            and not region
        ):

            rejection_reason = "MISSING_REGION"

        # -----------------------------------------------
        # Handle rejected record
        # -----------------------------------------------

        if rejection_reason:

            rejected_row = dict(row)

            rejected_row["rejection_reason"] = (
                rejection_reason
            )

            rejected_records.append(
                rejected_row
            )

            continue

        # -----------------------------------------------
        # Record is valid
        # -----------------------------------------------

        seen_transaction_ids.add(
            transaction_id
        )

        valid_records.append({
            "transaction_id": transaction_id,

            "transaction_ts": normalized_timestamp,

            "merchant_id": merchant_id,

            "customer_id": (
                row.get("customer_id") or ""
            ).strip() or None,

            "payment_method_id": payment_method_id,

            "amount": float(amount),

            "currency": currency,

            "status": status,

            "is_fraud_flag": (
                str(
                    row.get(
                        "is_fraud_flag",
                        "0"
                    )
                ).strip() == "1"
            ),

            "device_type": (
                row.get("device_type") or ""
            ).strip() or None,

            "region": region
        })

    # -----------------------------------------------------
    # Write rejected records
    # -----------------------------------------------------

    rejected_key = write_rejected_records(
        rejected_records,
        key
    )

    # -----------------------------------------------------
    # Write processed Parquet
    # -----------------------------------------------------

    processed_key = write_parquet(
        valid_records,
        key
    )

    # -----------------------------------------------------
    # Return pipeline result
    # -----------------------------------------------------

    result = {
        "source_file": key,
        "total_valid_records": len(valid_records),
        "total_rejected_records": len(rejected_records),
        "processed_key": processed_key,
        "rejected_key": rejected_key
    }

    logger.info(
        "ETL completed: %s",
        json.dumps(result)
    )

    return {
        "statusCode": 200,
        "body": json.dumps(result)
    }
