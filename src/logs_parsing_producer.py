#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Kafka Producer for Mozilla Build Logs
Sends COMPLETE log files to Kafka (one message per file)
"""
import os
import time
import json
from pathlib import Path
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
LOG_DIR = os.getenv('INPUT_LOG_DIR', '/app/logs')
TOPIC = os.getenv('KAFKA_TOPIC', 'mozilla-build-logs')


def get_producer():
    """Connect to Kafka with retries"""
    print(f"🔌 Connecting to Kafka at {KAFKA_BROKER}...")
    
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                max_request_size=104857600,  # 100MB
                buffer_memory=67108864,      # 64MB
                compression_type='gzip',     # Compress large files
                acks='all',                  # Wait for all replicas
                retries=3
            )
            print("✓ Connected to Kafka!")
            return producer
        except Exception as e:
            retry_count += 1
            print(f"⚠️  Attempt {retry_count}/{max_retries} failed: {e}")
            if retry_count < max_retries:
                print(f"   Retrying in 5 seconds...")
                time.sleep(5)
            else:
                raise Exception(f"Failed to connect to Kafka after {max_retries} attempts")


def send_complete_file(producer, filepath):
    """
    Send COMPLETE log file content as a single message
    
    Args:
        producer: Kafka producer instance
        filepath: Path to log file
    """
    filename = os.path.basename(filepath)
    log_id = Path(filepath).stem
    
    print(f"\n📄 Processing: {filename}")
    
    # Read COMPLETE file content
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    if not content:
        print(f"   ⚠️  File is empty, skipping")
        return False
    
    file_size = len(content)
    print(f"   Size: {file_size:,} characters ({file_size / 1024 / 1024:.2f} MB)")
    
    # Create message with COMPLETE content
    message = {
        'log_id': log_id,
        'filename': filename,
        'content': content,  # ← COMPLETE FILE CONTENT
        'file_size_bytes': os.path.getsize(filepath),
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    
    print(f"   📤 Sending to topic '{TOPIC}'...")
    
    try:
        # Send to Kafka
        future = producer.send(TOPIC, value=message)
        
        # Wait for confirmation
        record_metadata = future.get(timeout=30)
        
        print(f"   ✓ Sent successfully!")
        print(f"      Partition: {record_metadata.partition}")
        print(f"      Offset: {record_metadata.offset}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error sending: {e}")
        return False


def main():
    """Main producer logic"""
    print("=" * 70)
    print("Mozilla Build Log Producer - COMPLETE FILE MODE")
    print("=" * 70)
    
    # Get producer
    producer = get_producer()
    
    # Find log files
    log_files = [f for f in os.listdir(LOG_DIR) if f.endswith('.txt')]
    
    if not log_files:
        print(f"\n⚠️  No .txt files found in {LOG_DIR}")
        return
    
    print(f"\n📁 Found {len(log_files)} log file(s)")
    print(f"📂 Directory: {LOG_DIR}")
    print(f"📤 Target topic: {TOPIC}")
    
    print("\n" + "=" * 70)
    print("SENDING FILES")
    print("=" * 70)
    
    # Send files
    sent_count = 0
    failed_count = 0
    
    for idx, filename in enumerate(log_files, 1):
        filepath = os.path.join(LOG_DIR, filename)
        print(f"\n[{idx}/{len(log_files)}]")
        
        if send_complete_file(producer, filepath):
            sent_count += 1
        else:
            failed_count += 1
    
    # Flush all messages
    print("\n" + "=" * 70)
    print("🔄 Flushing pending messages...")
    producer.flush()
    producer.close()
    print("✓ All messages sent!")
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    print(f"✓ Successfully sent: {sent_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"📝 Total: {len(log_files)}")
    print("=" * 70)


if __name__ == "__main__":
    main()