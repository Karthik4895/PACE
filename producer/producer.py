import json
import requests
from kafka import KafkaProducer
from requests.auth import HTTPBasicAuth
import time
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configuration
POLL_INTERVAL = 60
JIRA_DOMAIN = os.getenv("JIRA_DOMAIN")
JIRA_USER = os.getenv("JIRA_USER")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

def format_jira_date(date_str):
    if not date_str:
        return None
    dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    return dt.strftime("%Y-%m-%d %H:%M")

def get_jira_tickets(last_updated=None):
    url = f"https://{JIRA_DOMAIN}/rest/api/2/search"
    headers = {"Content-Type": "application/json"}
    auth = HTTPBasicAuth(JIRA_USER, JIRA_API_TOKEN)
    
    jql = "ORDER BY updated DESC" 
    if last_updated:
        jql_date = format_jira_date(last_updated)
        jql = f"updated >= '{jql_date}' ORDER BY updated DESC"
    
    params = {
        "jql": jql,
        "fields": "id,key,summary,assignee,priority,status,created,updated,resolutiondate,customfield_10016"
    }
    
    response = requests.get(url, headers=headers, auth=auth, params=params)
    response.raise_for_status()
    return response.json().get("issues", [])

def create_kafka_producer():
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(2, 8, 1),
                request_timeout_ms=30000
            )
            print("Connected to Kafka")
            return producer
        except Exception as e:
            retry_count += 1
            wait_time = 2 ** retry_count
            print(f"Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(wait_time)
    
    raise RuntimeError("Failed to connect to Kafka after multiple attempts")

def main():
    producer = create_kafka_producer()
    last_updated = None
    
    while True:
        try:
            issues = get_jira_tickets(last_updated)
            
            if issues:
                last_updated = issues[0]['fields']['updated']
                print(f"Processing {len(issues)} updated tickets since {last_updated}")
                
                for issue in issues:
                    ticket = {
                        "id": issue["id"],
                        "ticket_id": issue["key"],
                        "summary": issue["fields"]["summary"],
                        "assignee": issue["fields"].get("assignee", {}).get("displayName"),
                        "priority": issue["fields"].get("priority", {}).get("name"),
                        "status": issue["fields"]["status"]["name"],
                        "created_at": issue["fields"]["created"],
                        "resolved_at": issue["fields"].get("resolutiondate"),
                        "sprint": None,
                        "story_points": issue["fields"].get("customfield_10016"),
                        "days_to_resolve": None
                    }
                    
                    producer.send(KAFKA_TOPIC, ticket)
                    print(f"Sent {ticket['ticket_id']} to Kafka")
            
            time.sleep(POLL_INTERVAL)
            
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()