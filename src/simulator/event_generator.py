"""
StreamMart Event Generator
Simulates realistic e-commerce user behavior and generates synthetic events

WHY THIS EXISTS:
- Simulates production-like event streams for testing and development
- No need for real user traffic to build/test the pipeline
- Generates realistic patterns: browsing, cart abandonment, purchases

WHAT BREAKS WITHOUT IT:
- No data to test the pipeline
- Can't validate streaming logic without events
- No way to demonstrate the system

REAL-WORLD ANALOG:
- In production, this would be your web/mobile app generating clickstream data
- Companies like Amazon, Netflix generate millions of events per second
"""

import uuid
import time
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from faker import Faker
import signal
import sys

from event_models import (
    PageViewEvent, ProductClickEvent, AddToCartEvent,
    PurchaseEvent, AbandonmentEvent,
    PurchaseItem, Address, CartItem,
    PageType, DeviceType, ListType, PaymentMethod, AbandonmentStage,
    PRODUCT_CATALOG, USER_BEHAVIOR_PROFILES
)
from kafka_producer import (
    StreamMartProducer,
    produce_pageview, produce_product_click, produce_add_to_cart,
    produce_purchase, produce_abandonment
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker()


class UserSession:
    """
    Represents a single user shopping session
    Maintains state across multiple events
    """
    
    def __init__(self, profile_type: str):
        self.session_id = str(uuid.uuid4())
        self.user_id = str(uuid.uuid4()) if random.random() > 0.3 else None  # 70% logged in
        self.profile = USER_BEHAVIOR_PROFILES[profile_type]
        self.device_type = random.choice(list(DeviceType)).value
        self.user_agent = fake.user_agent()
        self.ip_address = fake.ipv4()
        self.start_time = datetime.now()
        self.cart: List[Dict] = []
        self.cart_total = 0.0
        self.viewed_products: List[Dict] = []
        self.clicked_products: List[Dict] = []
        
    def get_session_duration(self) -> int:
        """Get session duration in seconds"""
        return int((datetime.now() - self.start_time).total_seconds())
    
    def add_to_cart_action(self, product: Dict) -> Tuple[float, int]:
        """Add product to cart and return new totals"""
        self.cart.append(product)
        self.cart_total += product['price']
        return self.cart_total, len(self.cart)


class EventGenerator:
    """Main event generation engine"""
    
    def __init__(self, events_per_second: int = 10):
        self.events_per_second = events_per_second
        self.producer = StreamMartProducer()
        self.active_sessions: List[UserSession] = []
        self.total_events_generated = 0
        self.running = True
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Event Generator initialized - Target: {events_per_second} events/sec")
    
    def _signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully"""
        logger.info("Shutdown signal received. Flushing events...")
        self.running = False
    
    def _create_new_session(self) -> UserSession:
        """Create a new user session with random profile"""
        profile_type = random.choices(
            list(USER_BEHAVIOR_PROFILES.keys()),
            weights=[0.5, 0.3, 0.2]  # 50% browsers, 30% researchers, 20% buyers
        )[0]
        return UserSession(profile_type)
    
    def _generate_pageview(self, session: UserSession, page_type: PageType) -> PageViewEvent:
        """Generate a page view event"""
        timestamp_ms = int(time.time() * 1000)
        
        page_urls = {
            PageType.HOME: "https://streammart.com/",
            PageType.CATEGORY: f"https://streammart.com/category/{fake.word()}",
            PageType.PRODUCT: f"https://streammart.com/product/{fake.uuid4()}",
            PageType.CART: "https://streammart.com/cart",
            PageType.CHECKOUT: "https://streammart.com/checkout",
            PageType.SEARCH: f"https://streammart.com/search?q={fake.word()}",
        }
        
        return PageViewEvent(
            event_id=str(uuid.uuid4()),
            session_id=session.session_id,
            user_id=session.user_id,
            timestamp=timestamp_ms,
            page_url=page_urls.get(page_type, page_urls[PageType.HOME]),
            page_type=page_type.value,
            referrer=random.choice([None, "https://google.com", "https://facebook.com"]),
            device_type=session.device_type,
            user_agent=session.user_agent,
            ip_address=session.ip_address
        )
    
    def _generate_product_click(self, session: UserSession, product: Dict) -> ProductClickEvent:
        """Generate a product click event"""
        timestamp_ms = int(time.time() * 1000)
        
        return ProductClickEvent(
            event_id=str(uuid.uuid4()),
            session_id=session.session_id,
            user_id=session.user_id,
            timestamp=timestamp_ms,
            product_id=product['id'],
            product_name=product['name'],
            product_category=product['category'],
            product_price=product['price'],
            click_position=random.randint(1, 20),
            list_type=random.choice(list(ListType)).value,
            device_type=session.device_type
        )
    
    def _generate_add_to_cart(self, session: UserSession, product: Dict) -> AddToCartEvent:
        """Generate add to cart event"""
        timestamp_ms = int(time.time() * 1000)
        quantity = random.randint(1, 3)
        cart_total, cart_count = session.add_to_cart_action(product)
        
        return AddToCartEvent(
            event_id=str(uuid.uuid4()),
            session_id=session.session_id,
            user_id=session.user_id,
            timestamp=timestamp_ms,
            product_id=product['id'],
            product_name=product['name'],
            product_category=product['category'],
            product_price=product['price'],
            quantity=quantity,
            cart_total=cart_total,
            cart_item_count=cart_count,
            device_type=session.device_type
        )
    
    def _generate_purchase(self, session: UserSession) -> PurchaseEvent:
        """Generate purchase event"""
        timestamp_ms = int(time.time() * 1000)
        
        # Convert cart to purchase items
        items = []
        for product in session.cart:
            quantity = random.randint(1, 2)
            unit_price = product['price']
            total_price = unit_price * quantity
            
            items.append({
                'product_id': product['id'],
                'product_name': product['name'],
                'product_category': product['category'],
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': total_price
            })
        
        subtotal = sum(item['total_price'] for item in items)
        tax = round(subtotal * 0.08, 2)  # 8% tax
        shipping = 0.0 if subtotal > 50 else 9.99
        total = subtotal + tax + shipping
        
        return PurchaseEvent(
            event_id=str(uuid.uuid4()),
            session_id=session.session_id,
            user_id=session.user_id,
            timestamp=timestamp_ms,
            order_id=f"ORD-{uuid.uuid4().hex[:12].upper()}",
            items=items,
            subtotal=subtotal,
            tax=tax,
            shipping=shipping,
            total=total,
            payment_method=random.choice(list(PaymentMethod)).value,
            shipping_address={
                'country': 'US',
                'state': fake.state_abbr(),
                'city': fake.city(),
                'zipcode': fake.zipcode()
            },
            device_type=session.device_type
        )
    
    def _generate_abandonment(self, session: UserSession) -> AbandonmentEvent:
        """Generate cart abandonment event"""
        timestamp_ms = int(time.time() * 1000)
        
        cart_items = []
        for product in session.cart:
            cart_items.append({
                'product_id': product['id'],
                'product_name': product['name'],
                'product_category': product['category'],
                'quantity': random.randint(1, 2),
                'price': product['price']
            })
        
        return AbandonmentEvent(
            event_id=str(uuid.uuid4()),
            session_id=session.session_id,
            user_id=session.user_id,
            timestamp=timestamp_ms,
            cart_items=cart_items,
            cart_total=session.cart_total,
            abandonment_stage=random.choice(list(AbandonmentStage)).value,
            session_duration_seconds=session.get_session_duration(),
            device_type=session.device_type
        )
    
    def _simulate_session(self, session: UserSession):
        """Simulate a complete user session"""
        # Calculate sleep factor based on target rate
        # At high rates (>20 events/sec), skip sleeps for throughput
        # At low rates, use realistic timing
        if self.events_per_second >= 20:
            sleep_factor = 0.0  # No delays at high throughput
        else:
            sleep_factor = min(1.0, 10.0 / self.events_per_second)
        
        try:
            # 1. Homepage visit
            event = self._generate_pageview(session, PageType.HOME)
            produce_pageview(self.producer, event)
            self.total_events_generated += 1
            if sleep_factor > 0:
                time.sleep(random.uniform(0.5, 2.0) * sleep_factor)
            
            # 2. Browse products based on profile
            num_products_to_view = random.randint(
                2, session.profile['avg_products_viewed']
            )
            
            for _ in range(num_products_to_view):
                if not self.running:
                    break
                    
                # Category page view
                event = self._generate_pageview(session, PageType.CATEGORY)
                produce_pageview(self.producer, event)
                self.total_events_generated += 1
                if sleep_factor > 0:
                    time.sleep(random.uniform(0.3, 1.5) * sleep_factor)
                
                # Product clicks
                if random.random() < 0.6:  # 60% click on products
                    product = random.choice(PRODUCT_CATALOG)
                    session.clicked_products.append(product)
                    
                    # Product page view
                    event = self._generate_pageview(session, PageType.PRODUCT)
                    produce_pageview(self.producer, event)
                    self.total_events_generated += 1
                    if sleep_factor > 0:
                        time.sleep(random.uniform(0.2, 0.8) * sleep_factor)
                    
                    # Product click
                    event = self._generate_product_click(session, product)
                    produce_product_click(self.producer, event)
                    self.total_events_generated += 1
                    if sleep_factor > 0:
                        time.sleep(random.uniform(0.5, 2.0) * sleep_factor)
                    
                    # Add to cart?
                    if random.random() < session.profile['add_to_cart_probability']:
                        event = self._generate_add_to_cart(session, product)
                        produce_add_to_cart(self.producer, event)
                        self.total_events_generated += 1
                        if sleep_factor > 0:
                            time.sleep(random.uniform(1.0, 3.0) * sleep_factor)
            
            # 3. Purchase or abandon?
            if len(session.cart) > 0:
                if random.random() < session.profile['purchase_probability']:
                    # View cart
                    event = self._generate_pageview(session, PageType.CART)
                    produce_pageview(self.producer, event)
                    self.total_events_generated += 1
                    if sleep_factor > 0:
                        time.sleep(random.uniform(2.0, 5.0) * sleep_factor)
                    
                    # Checkout page
                    event = self._generate_pageview(session, PageType.CHECKOUT)
                    produce_pageview(self.producer, event)
                    self.total_events_generated += 1
                    if sleep_factor > 0:
                        time.sleep(random.uniform(3.0, 8.0) * sleep_factor)
                    
                    # Complete purchase
                    event = self._generate_purchase(session)
                    produce_purchase(self.producer, event)
                    self.total_events_generated += 1
                    logger.info(f"💰 Purchase completed: {event.order_id} - ${event.total:.2f}")
                else:
                    # Abandon cart
                    event = self._generate_abandonment(session)
                    produce_abandonment(self.producer, event)
                    self.total_events_generated += 1
                    logger.info(f"🛒 Cart abandoned: {len(session.cart)} items - ${session.cart_total:.2f}")
                    
        except Exception as e:
            logger.error(f"Error simulating session: {e}", exc_info=True)
    
    def run(self, duration_seconds=0):
        """
        Main event generation loop
        
        Args:
            duration_seconds (int): How long to run in seconds. 0 = run indefinitely (default)
        """
        logger.info("🚀 Starting event generation...")
        logger.info(f"Target rate: {self.events_per_second} events/second")
        if duration_seconds > 0:
            logger.info(f"Duration: {duration_seconds} seconds")
        else:
            logger.info("Duration: Indefinite (press Ctrl+C to stop)")
        
        start_time = time.time()
        last_stats_time = start_time
        end_time = start_time + duration_seconds if duration_seconds > 0 else float('inf')
        
        # Calculate how many sessions we need
        # Estimate ~10 events per session
        avg_events_per_session = 10
        target_sessions_per_sec = max(1, self.events_per_second / avg_events_per_session)
        
        try:
            while self.running and time.time() < end_time:
                current_time = time.time()
                
                # Create sessions to maintain target rate
                # At high rates, create multiple sessions per iteration
                sessions_to_create = max(1, int(target_sessions_per_sec * 0.5))
                for _ in range(sessions_to_create):
                    session = self._create_new_session()
                    self.active_sessions.append(session)
                
                # Simulate sessions and generate events
                sessions_to_process = list(self.active_sessions)
                self.active_sessions.clear()
                
                for session in sessions_to_process:
                    if not self.running:
                        break
                    self._simulate_session(session)
                
                # Poll producer to handle delivery reports (non-blocking)
                self.producer.producer.poll(0)
                
                # Brief sleep to prevent CPU spinning
                if self.events_per_second < 50:
                    time.sleep(0.1)
                else:
                    time.sleep(0.01)
                
                # Print stats every 10 seconds
                if current_time - last_stats_time >= 10:
                    elapsed = current_time - start_time
                    rate = self.total_events_generated / elapsed if elapsed > 0 else 0
                    remaining = end_time - current_time if duration_seconds > 0 else None
                    stats_msg = (
                        f"📊 Stats: {self.total_events_generated} events generated | "
                        f"Rate: {rate:.2f} events/sec | "
                        f"Active sessions: {len(self.active_sessions)}"
                    )
                    if remaining and remaining > 0:
                        stats_msg += f" | Time remaining: {int(remaining)}s"
                    logger.info(stats_msg)
                    last_stats_time = current_time
            
            # Check if we finished because duration expired
            if duration_seconds > 0 and time.time() >= end_time:
                logger.info(f"⏱️  Duration completed: {duration_seconds} seconds")
                    
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received")
        except Exception as e:
            logger.error(f"Fatal error in event generator: {e}", exc_info=True)
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down event generator...")
        self.running = False
        
        # Flush remaining events
        self.producer.close()
        
        # Print final stats
        logger.info(f"✅ Total events generated: {self.total_events_generated}")
        logger.info("Event generator stopped")


def main():
    """Entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='StreamMart Event Generator - Simulates realistic e-commerce events'
    )
    parser.add_argument(
        '--rate',
        type=int,
        default=100,
        help='Target events per second (default: 100)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=0,
        help='Duration in seconds. Use 0 for indefinite (default: 0)'
    )
    
    args = parser.parse_args()
    
    generator = EventGenerator(events_per_second=args.rate)
    generator.run(duration_seconds=args.duration)


if __name__ == '__main__':
    main()
