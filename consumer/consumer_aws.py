import json
import os
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_TOPIC = os.getenv("AWS_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="aws-cost-consumer"
    )
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    print(f"Listening to {KAFKA_TOPIC}")
    for message in consumer:
        data = message.value
        print(f"Received: {data}")
        try:
            cursor.execute("""
                INSERT INTO aws_costs (
                    billing_month, service_name, usage_type,
                    usage_quantity, unblended_cost, currency
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (billing_month, service_name, usage_type) DO UPDATE SET
                    usage_quantity = EXCLUDED.usage_quantity,
                    unblended_cost = EXCLUDED.unblended_cost,
                    currency = EXCLUDED.currency
            """, (
                data["billing_month"],
                data["service_name"],
                data["usage_type"],
                data["usage_quantity"],
                data["unblended_cost"],
                data["currency"]
            ))
            conn.commit()
            print("Data committed")
        except Exception as e:
            print(f"Insert failed: {e}")
            conn.rollback()

if __name__ == "__main__":
    main()
