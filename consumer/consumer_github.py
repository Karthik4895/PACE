import os
import json
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_TOPIC = os.getenv("GITHUB_TOPIC")
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
        group_id="github-consumer"
    )

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    print(f"Listening to topic: {KAFKA_TOPIC}")
    for message in consumer:
        data = message.value
        print(f"Received: {data}")
        try:
            cursor.execute("""
                INSERT INTO github_commits (
                    repo_name, commit_sha, author_name,
                    commit_message, commit_date, url
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (commit_sha) DO NOTHING
            """, (
                data["repo_name"],
                data["commit_sha"],
                data["author_name"],
                data["commit_message"],
                data["commit_date"],
                data["url"]
            ))
            conn.commit()
            print(f"Stored commit {data['commit_sha']}")
        except Exception as e:
            print(f"Failed to insert: {e}")
            conn.rollback()

if __name__ == "__main__":
    main()
