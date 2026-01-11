import os
import json
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
OUTPUT_FILE = os.path.join(os.getenv('OUTPUT_DIR', '/app/output'), 'structured_logs.jsonl')
TOPIC = 'mozilla-build-logs'

def main():
    print(f"Starting Consumer... saving to {OUTPUT_FILE}")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Open file in append mode
    with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
        for message in consumer:
            log_data = message.value
            
            # Here you would typically insert into ElasticSearch or Database
            # For now, we save to a structured file
            f.write(json.dumps(log_data) + "\n")
            f.flush()
            
            print(f"Processed: [{log_data.get('log_level', 'EVENT')}] {log_data.get('message')[:50]}...")

if __name__ == "__main__":
    main()