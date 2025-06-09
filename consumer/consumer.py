import json
import time
import os
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# Initialize PostgreSQL connection
def create_db_connection():
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            print("Connected to PostgreSQL")
            return conn
        except Exception as e:
            retry_count += 1
            wait_time = 2 ** retry_count
            print(f"Failed to connect to PostgreSQL (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(wait_time)
    
    raise RuntimeError("Failed to connect to PostgreSQL after multiple attempts")

# Initialize Kafka Consumer
def create_kafka_consumer():
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:

        try:
            topic = os.getenv("AWS_TOPIC")
            if not topic:
                raise ValueError("AWS_TOPIC env variable is missing!")
            consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='jira-consumer-group'
)
            print(f"Connected to Kafka and subscribed to {KAFKA_TOPIC}")
            return consumer
        except Exception as e:
            retry_count += 1
            wait_time = 2 ** retry_count
            print(f"Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(wait_time)
    
    raise RuntimeError("Failed to connect to Kafka after multiple attempts")


def main():
    conn = create_db_connection()
    consumer = create_kafka_consumer()
    
    print("Starting poll loop...")
    print(f"Assigned partitions: {consumer.assignment()}")
    
    with conn.cursor() as cursor:
        for message in consumer:
            print(f"\nraw message: {message}")
            ticket = message.value
            
            try:
                cursor.execute("""
                    INSERT INTO tickets (
                        id, ticket_id, summary, assignee, priority,
                        status, created_at, resolved_at, sprint,
                        story_points, days_to_resolve
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        ticket_id = EXCLUDED.ticket_id,
                        summary = EXCLUDED.summary,
                        assignee = EXCLUDED.assignee,
                        priority = EXCLUDED.priority,
                        status = EXCLUDED.status,
                        resolved_at = EXCLUDED.resolved_at,
                        sprint = EXCLUDED.sprint,
                        story_points = EXCLUDED.story_points,
                        days_to_resolve = EXCLUDED.days_to_resolve
                        """, (
                    ticket["id"],
                    ticket["ticket_id"],
                    ticket["summary"],
                    ticket["assignee"],
                    ticket["priority"],
                    ticket["status"],
                    ticket["created_at"],
                    ticket["resolved_at"],
                    ticket["sprint"],
                    ticket["story_points"],
                    ticket["days_to_resolve"]
                ))
                conn.commit()
                print(f"Saved/Updated ticket {ticket['ticket_id']}")
            except Exception as e:
                print(f"Failed to save ticket {ticket['ticket_id']}: {e}")
                conn.rollback()

if __name__ == "__main__":
    main()