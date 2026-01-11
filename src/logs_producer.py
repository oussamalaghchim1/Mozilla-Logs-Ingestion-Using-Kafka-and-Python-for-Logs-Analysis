import os
import time
import json
import re
from kafka import KafkaProducer

# Configuration from Environment Variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
LOG_DIR = os.getenv('INPUT_LOG_DIR', '/app/logs')
TOPIC = 'mozilla-build-logs'

# Regex for standard log lines (e.g., "16:37:15 INFO - ...")
LOG_PATTERN = re.compile(r'^(\d{2}:\d{2}:\d{2})\s+(INFO|WARNING|ERROR)\s+-\s+(.*)$')

def get_producer():
    # Wait for Kafka to start up
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("Connected to Kafka!")
            return producer
        except Exception as e:
            print(f"Waiting for Kafka... ({e})")
            time.sleep(5)

def parse_file(filepath):
    """Parses a single file and yields structured log dicts."""
    filename = os.path.basename(filepath)
    context = {"source_file": filename}
    
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        current_buffer = None
        
        for line in f:
            line = line.strip()
            if not line: continue

            # 1. Grab Metadata (Builder, Slave, etc.)
            if line.startswith("builder:"): context["builder"] = line.split(":", 1)[1].strip(); continue
            if line.startswith("buildid:"): context["buildid"] = line.split(":", 1)[1].strip(); continue
            if line.startswith("results:"): context["result"] = line.split(":", 1)[1].strip(); continue

            # 2. Check for Timestamped Log Line
            match = LOG_PATTERN.match(line)
            if match:
                if current_buffer: yield current_buffer
                timestamp, level, msg = match.groups()
                current_buffer = {
                    "timestamp": timestamp, "log_level": level, 
                    "message": msg, **context
                }
            # 3. Check for Phase Delimiters
            elif line.startswith("========= Started"):
                if current_buffer: yield current_buffer; current_buffer = None
                yield {"type": "PHASE_START", "message": line, **context}
            # 4. Handle Stack Traces (Append to previous)
            elif current_buffer:
                current_buffer["message"] += f"\n{line}"
        
        if current_buffer: yield current_buffer

def main():
    producer = get_producer()
    files = [f for f in os.listdir(LOG_DIR) if f.endswith('.txt')]
    print(f"Found {len(files)} log files.")

    for filename in files:
        filepath = os.path.join(LOG_DIR, filename)
        print(f"Streaming {filename}...")
        
        for log_entry in parse_file(filepath):
            producer.send(TOPIC, value=log_entry)
            # simulate real-time streaming (remove or lower for speed)
            time.sleep(0.01) 
            
        producer.flush()
    print("All files streamed.")

if __name__ == "__main__":
    main()