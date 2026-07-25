"""
Kafka Producer for StreamMart Events
Publishes JSON events to Kafka topics for Spark processing
"""

import json
import logging
from typing import Dict, Any
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StreamMartProducer:
    """
    Kafka producer with JSON event publishing
    
    Why this exists: Produces predictable JSON payloads that Spark can parse
    consistently across all streaming jobs.
    
    What breaks without it: Parsing errors downstream and empty aggregations.
    """
    
    def __init__(self):
        self.kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
        
        # Initialize Kafka producer
        producer_config = {
            'bootstrap.servers': self.kafka_broker,
            'client.id': 'streammart-event-generator',
            'acks': 'all',  # Wait for all replicas to acknowledge
            'compression.type': 'snappy',
            'linger.ms': 10,  # Batch messages for 10ms
            'batch.size': 16384,  # 16KB batch size
            'max.in.flight.requests.per.connection': 5,
            'retries': 3,
        }
        logger.info(f"Producer config bootstrap.servers: {producer_config['bootstrap.servers']}")
        self.producer = Producer(producer_config)
        
        self.string_serializer = StringSerializer('utf_8')
        
        logger.info(f"StreamMart Producer initialized - Broker: {self.kafka_broker}")
    
    def _delivery_report(self, err, msg):
        """Callback for delivery reports"""
        if err:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(
                f'Message delivered to {msg.topic()} '
                f'[partition {msg.partition()}] at offset {msg.offset()}'
            )
    
    def produce_event(self, event_type: str, key: str, value: Dict[str, Any]) -> bool:
        """
        Produce event to Kafka with Avro serialization
        
        Args:
            event_type: Type of event (pageview, product_click, etc.)
            key: Partition key (typically session_id)
            value: Event data as dictionary
        
        Returns:
            True if successful, False otherwise
        """
        topic = f"events.{event_type}"
        
        try:
            # Serialize key and value. We publish JSON payloads so Spark can parse
            # events with from_json in structured streaming jobs.
            serialized_key = self.string_serializer(key)
            serialized_value = json.dumps(value).encode('utf-8')
            
            # Produce to Kafka
            self.producer.produce(
                topic=topic,
                key=serialized_key,
                value=serialized_value,
                on_delivery=self._delivery_report
            )
            
            # Trigger delivery report callbacks
            self.producer.poll(0)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to produce {event_type} event: {e}")
            # In production, send to DLQ topic
            self._send_to_dlq(event_type, key, value, str(e))
            return False
    
    def _send_to_dlq(self, event_type: str, key: str, value: Dict[str, Any], error: str):
        """Send failed event to Dead Letter Queue"""
        dlq_payload = {
            'original_topic': f'events.{event_type}',
            'key': key,
            'value': value,
            'error': error,
            'timestamp': value.get('timestamp')
        }
        
        try:
            self.producer.produce(
                topic='events.dlq',
                key=self.string_serializer(key),
                value=json.dumps(dlq_payload).encode('utf-8')
            )
            logger.warning(f"Event sent to DLQ: {event_type}")
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
    
    def flush(self):
        """Wait for all messages to be delivered"""
        remaining = self.producer.flush(timeout=30)
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered")
        else:
            logger.info("All messages delivered successfully")
    
    def close(self):
        """Close producer and flush remaining messages"""
        logger.info("Closing producer...")
        self.flush()
        logger.info("Producer closed")


# Convenience functions for producing specific event types

def produce_pageview(producer: StreamMartProducer, event):
    return producer.produce_event('pageview', event.session_id, event.to_dict())


def produce_product_click(producer: StreamMartProducer, event):
    return producer.produce_event('product_click', event.session_id, event.to_dict())


def produce_add_to_cart(producer: StreamMartProducer, event):
    return producer.produce_event('add_to_cart', event.session_id, event.to_dict())


def produce_purchase(producer: StreamMartProducer, event):
    return producer.produce_event('purchase', event.session_id, event.to_dict())


def produce_abandonment(producer: StreamMartProducer, event):
    return producer.produce_event('abandonment', event.session_id, event.to_dict())
