import json
import os
import boto3
from kafka import KafkaProducer
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY")

KAFKA_TOPIC = os.getenv("AWS_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

def get_aws_cost_data():
    print("Connecting to AWS Cost Explorer...")
    try:
        ce = boto3.client("ce", region_name="us-east-1")

        now = datetime.utcnow()
        start_date = "2025-06-01"
        end_date = "2025-06-30"

        print(f"Time period: {start_date} to {end_date}")
        print("Sending get_cost_and_usage request...")

        response = ce.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="MONTHLY",
            Metrics=["UsageQuantity", "UnblendedCost"],
            GroupBy=[
                {"Type": "DIMENSION", "Key": "SERVICE"},
                {"Type": "DIMENSION", "Key": "USAGE_TYPE"}
            ]
        )
        print("AWS Cost Explorer response received",response)
        

        records = []
        for group in response["ResultsByTime"][0]["Groups"]:
            service_name = group["Keys"][0]
            usage_type = group["Keys"][1]
            usage_quantity = float(group["Metrics"]["UsageQuantity"]["Amount"])
            unblended_cost = float(group["Metrics"]["UnblendedCost"]["Amount"])

            record = {
                "billing_month": start_date,
                "service_name": service_name,
                "usage_type": usage_type,
                "usage_quantity": usage_quantity,
                "unblended_cost": unblended_cost,
                "currency": "USD"
            }
            records.append(record)
        print("Fetching records",records)
        return records

    except Exception as e:
        print(f"AWS error: {e}")
        raise


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Fetching AWS Cost Data...",producer)
    for record in get_aws_cost_data():
        print("record",record)
        producer.send(KAFKA_TOPIC, record)
        print(f"Sent: {record}")
    producer.flush()

if __name__ == "__main__":
    main()
