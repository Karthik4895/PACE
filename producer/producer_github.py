import os
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

GITHUB_USERNAME = os.getenv("GITHUB_USERNAME")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO = os.getenv("GITHUB_REPO")
KAFKA_TOPIC = os.getenv("GITHUB_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

def fetch_commits():
    url = f"https://api.github.com/repos/{REPO}/commits"
    headers = {"Accept": "application/vnd.github.v3+json"}
    response = requests.get(url, auth=(GITHUB_USERNAME, GITHUB_TOKEN), headers=headers)
    response.raise_for_status()
    return response.json()

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    commits = fetch_commits()
    for commit in commits:
        data = {
            "repo_name": REPO,
            "commit_sha": commit["sha"],
            "author_name": commit["commit"]["author"]["name"],
            "commit_message": commit["commit"]["message"],
            "commit_date": commit["commit"]["author"]["date"],
            "url": commit["html_url"]
        }
        producer.send(KAFKA_TOPIC, data)
        print(f"Sent commit {data['commit_sha']}")
    producer.flush()

if __name__ == "__main__":
    main()
