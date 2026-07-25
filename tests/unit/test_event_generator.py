"""
Unit tests for Event Generator
Tests event creation logic and data models
"""

import pytest
from datetime import datetime

# src/simulator is added to sys.path by tests/conftest.py
from event_models import (
    PageViewEvent, ProductClickEvent, AddToCartEvent,
    PurchaseEvent, AbandonmentEvent,
    PRODUCT_CATALOG, USER_BEHAVIOR_PROFILES
)


class TestEventModels:
    """Test event data models"""
    
    def test_pageview_event_creation(self):
        """Test PageViewEvent can be created with valid data"""
        event = PageViewEvent(
            event_id="test-123",
            session_id="session-456",
            timestamp=int(datetime.now().timestamp() * 1000),
            page_url="https://streammart.com/",
            page_type="HOME",
            device_type="DESKTOP",
            user_agent="Mozilla/5.0",
            ip_address="192.168.1.1"
        )
        
        assert event.event_id == "test-123"
        assert event.session_id == "session-456"
        assert event.page_type == "HOME"
        assert event.device_type == "DESKTOP"
    
    def test_pageview_to_dict(self):
        """Test event can be converted to dict"""
        event = PageViewEvent(
            event_id="test-123",
            session_id="session-456",
            timestamp=1234567890000,
            page_url="https://streammart.com/",
            page_type="HOME",
            device_type="DESKTOP",
            user_agent="Mozilla/5.0",
            ip_address="192.168.1.1"
        )
        
        event_dict = event.to_dict()
        assert isinstance(event_dict, dict)
        assert event_dict['event_id'] == "test-123"
        assert event_dict['page_type'] == "HOME"
    
    def test_product_click_event(self):
        """Test ProductClickEvent creation"""
        product = PRODUCT_CATALOG[0]
        
        event = ProductClickEvent(
            event_id="click-123",
            session_id="session-456",
            timestamp=1234567890000,
            product_id=product['id'],
            product_name=product['name'],
            product_category=product['category'],
            product_price=product['price'],
            click_position=1,
            list_type="CATEGORY_PAGE",
            device_type="MOBILE"
        )
        
        assert event.product_id == product['id']
        assert event.click_position == 1
        assert event.device_type == "MOBILE"
    
    def test_add_to_cart_event(self):
        """Test AddToCartEvent creation"""
        product = PRODUCT_CATALOG[0]
        
        event = AddToCartEvent(
            event_id="cart-123",
            session_id="session-456",
            timestamp=1234567890000,
            product_id=product['id'],
            product_name=product['name'],
            product_category=product['category'],
            product_price=product['price'],
            quantity=2,
            cart_total=1199.98,
            cart_item_count=2,
            device_type="DESKTOP"
        )
        
        assert event.quantity == 2
        assert event.cart_total == 1199.98
        assert event.cart_item_count == 2
    
    def test_purchase_event(self):
        """Test PurchaseEvent creation"""
        items = [
            {
                'product_id': 'PROD-001',
                'product_name': 'Test Product',
                'product_category': 'Electronics',
                'quantity': 1,
                'unit_price': 99.99,
                'total_price': 99.99
            }
        ]
        
        event = PurchaseEvent(
            event_id="purchase-123",
            session_id="session-456",
            timestamp=1234567890000,
            order_id="ORD-123456",
            items=items,
            subtotal=99.99,
            tax=8.00,
            shipping=0.00,
            total=107.99,
            payment_method="CREDIT_CARD",
            shipping_address={
                'country': 'US',
                'state': 'CA',
                'city': 'San Francisco',
                'zipcode': '94102'
            },
            device_type="DESKTOP"
        )
        
        assert event.order_id == "ORD-123456"
        assert event.total == 107.99
        assert len(event.items) == 1
    
    def test_abandonment_event(self):
        """Test AbandonmentEvent creation"""
        cart_items = [
            {
                'product_id': 'PROD-001',
                'product_name': 'Test Product',
                'product_category': 'Electronics',
                'quantity': 1,
                'price': 99.99
            }
        ]
        
        event = AbandonmentEvent(
            event_id="abandon-123",
            session_id="session-456",
            timestamp=1234567890000,
            cart_items=cart_items,
            cart_total=99.99,
            abandonment_stage="CART",
            session_duration_seconds=180,
            device_type="MOBILE"
        )
        
        assert event.cart_total == 99.99
        assert event.abandonment_stage == "CART"
        assert event.session_duration_seconds == 180


class TestProductCatalog:
    """Test product catalog"""
    
    def test_catalog_not_empty(self):
        """Ensure product catalog has items"""
        assert len(PRODUCT_CATALOG) > 0
    
    def test_catalog_products_have_required_fields(self):
        """Ensure each product has required fields"""
        for product in PRODUCT_CATALOG:
            assert 'id' in product
            assert 'name' in product
            assert 'category' in product
            assert 'price' in product
            assert product['price'] > 0
    
    def test_catalog_product_ids_unique(self):
        """Ensure product IDs are unique"""
        product_ids = [p['id'] for p in PRODUCT_CATALOG]
        assert len(product_ids) == len(set(product_ids))


class TestUserBehaviorProfiles:
    """Test user behavior profiles"""
    
    def test_profiles_exist(self):
        """Ensure behavior profiles are defined"""
        assert 'browser' in USER_BEHAVIOR_PROFILES
        assert 'researcher' in USER_BEHAVIOR_PROFILES
        assert 'buyer' in USER_BEHAVIOR_PROFILES
    
    def test_profiles_have_probabilities(self):
        """Ensure each profile has required probability fields"""
        for profile_name, profile in USER_BEHAVIOR_PROFILES.items():
            assert 'purchase_probability' in profile
            assert 'avg_products_viewed' in profile
            assert 'avg_products_clicked' in profile
            assert 'add_to_cart_probability' in profile
            
            # Probabilities should be between 0 and 1
            assert 0 <= profile['purchase_probability'] <= 1
            assert 0 <= profile['add_to_cart_probability'] <= 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
