"""
StreamMart Event Data Models
Defines the structure of all e-commerce events
"""

from dataclasses import dataclass, asdict
from typing import Optional, List
from enum import Enum


class PageType(Enum):
    HOME = "HOME"
    CATEGORY = "CATEGORY"
    PRODUCT = "PRODUCT"
    CART = "CART"
    CHECKOUT = "CHECKOUT"
    SEARCH = "SEARCH"


class DeviceType(Enum):
    DESKTOP = "DESKTOP"
    MOBILE = "MOBILE"
    TABLET = "TABLET"


class ListType(Enum):
    SEARCH_RESULTS = "SEARCH_RESULTS"
    CATEGORY_PAGE = "CATEGORY_PAGE"
    RECOMMENDATIONS = "RECOMMENDATIONS"
    BESTSELLERS = "BESTSELLERS"
    RELATED_PRODUCTS = "RELATED_PRODUCTS"


class PaymentMethod(Enum):
    CREDIT_CARD = "CREDIT_CARD"
    DEBIT_CARD = "DEBIT_CARD"
    PAYPAL = "PAYPAL"
    APPLE_PAY = "APPLE_PAY"
    GOOGLE_PAY = "GOOGLE_PAY"


class AbandonmentStage(Enum):
    CART = "CART"
    CHECKOUT = "CHECKOUT"
    PAYMENT = "PAYMENT"


@dataclass
class PageViewEvent:
    """Page view event - user lands on a page"""
    event_id: str
    session_id: str
    timestamp: int  # milliseconds since epoch
    page_url: str
    page_type: str
    device_type: str
    user_agent: str
    ip_address: str
    user_id: Optional[str] = None
    referrer: Optional[str] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class ProductClickEvent:
    """Product click event - user clicks on a product"""
    event_id: str
    session_id: str
    timestamp: int
    product_id: str
    product_name: str
    product_category: str
    product_price: float
    click_position: int
    list_type: str
    device_type: str
    user_id: Optional[str] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class AddToCartEvent:
    """Add to cart event - user adds product to cart"""
    event_id: str
    session_id: str
    timestamp: int
    product_id: str
    product_name: str
    product_category: str
    product_price: float
    quantity: int
    cart_total: float
    cart_item_count: int
    device_type: str
    user_id: Optional[str] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class PurchaseItem:
    """Individual item in a purchase"""
    product_id: str
    product_name: str
    product_category: str
    quantity: int
    unit_price: float
    total_price: float


@dataclass
class Address:
    """Shipping address"""
    country: str
    state: str
    city: str
    zipcode: str


@dataclass
class PurchaseEvent:
    """Purchase event - completed transaction"""
    event_id: str
    session_id: str
    timestamp: int
    order_id: str
    items: List[dict]  # List of PurchaseItem dicts
    subtotal: float
    tax: float
    shipping: float
    total: float
    payment_method: str
    shipping_address: dict  # Address dict
    device_type: str
    user_id: Optional[str] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class CartItem:
    """Item in abandoned cart"""
    product_id: str
    product_name: str
    product_category: str
    quantity: int
    price: float


@dataclass
class AbandonmentEvent:
    """Abandonment event - user leaves with items in cart"""
    event_id: str
    session_id: str
    timestamp: int
    cart_items: List[dict]  # List of CartItem dicts
    cart_total: float
    abandonment_stage: str
    session_duration_seconds: int
    device_type: str
    user_id: Optional[str] = None

    def to_dict(self):
        return asdict(self)


# Product catalog for simulation
PRODUCT_CATALOG = [
    # Electronics
    {"id": "ELEC-001", "name": "4K Smart TV 55\"", "category": "Electronics", "price": 599.99},
    {"id": "ELEC-002", "name": "Wireless Bluetooth Headphones", "category": "Electronics", "price": 149.99},
    {"id": "ELEC-003", "name": "Laptop 15.6\" Intel i7", "category": "Electronics", "price": 899.99},
    {"id": "ELEC-004", "name": "Smartphone 128GB", "category": "Electronics", "price": 699.99},
    {"id": "ELEC-005", "name": "Tablet 10\" WiFi", "category": "Electronics", "price": 329.99},
    {"id": "ELEC-006", "name": "Gaming Console", "category": "Electronics", "price": 499.99},
    {"id": "ELEC-007", "name": "Wireless Mouse", "category": "Electronics", "price": 29.99},
    {"id": "ELEC-008", "name": "Mechanical Keyboard", "category": "Electronics", "price": 89.99},
    {"id": "ELEC-009", "name": "Webcam 1080p", "category": "Electronics", "price": 79.99},
    {"id": "ELEC-010", "name": "External SSD 1TB", "category": "Electronics", "price": 119.99},
    
    # Clothing
    {"id": "CLTH-001", "name": "Men's Denim Jeans", "category": "Clothing", "price": 49.99},
    {"id": "CLTH-002", "name": "Women's Summer Dress", "category": "Clothing", "price": 39.99},
    {"id": "CLTH-003", "name": "Unisex Hoodie", "category": "Clothing", "price": 44.99},
    {"id": "CLTH-004", "name": "Running Shoes", "category": "Clothing", "price": 89.99},
    {"id": "CLTH-005", "name": "Leather Jacket", "category": "Clothing", "price": 199.99},
    {"id": "CLTH-006", "name": "Cotton T-Shirt Pack (3)", "category": "Clothing", "price": 24.99},
    {"id": "CLTH-007", "name": "Yoga Pants", "category": "Clothing", "price": 34.99},
    {"id": "CLTH-008", "name": "Winter Coat", "category": "Clothing", "price": 149.99},
    {"id": "CLTH-009", "name": "Baseball Cap", "category": "Clothing", "price": 19.99},
    {"id": "CLTH-010", "name": "Dress Shirt", "category": "Clothing", "price": 39.99},
    
    # Home & Kitchen
    {"id": "HOME-001", "name": "Coffee Maker", "category": "Home", "price": 79.99},
    {"id": "HOME-002", "name": "Blender 800W", "category": "Home", "price": 59.99},
    {"id": "HOME-003", "name": "Bedding Set Queen", "category": "Home", "price": 89.99},
    {"id": "HOME-004", "name": "Non-Stick Cookware Set", "category": "Home", "price": 129.99},
    {"id": "HOME-005", "name": "Vacuum Cleaner", "category": "Home", "price": 199.99},
    {"id": "HOME-006", "name": "Air Purifier", "category": "Home", "price": 149.99},
    {"id": "HOME-007", "name": "Bath Towel Set", "category": "Home", "price": 44.99},
    {"id": "HOME-008", "name": "Table Lamp", "category": "Home", "price": 34.99},
    {"id": "HOME-009", "name": "Wall Clock", "category": "Home", "price": 24.99},
    {"id": "HOME-010", "name": "Throw Pillows (4)", "category": "Home", "price": 39.99},
    
    # Sports & Outdoors
    {"id": "SPRT-001", "name": "Yoga Mat", "category": "Sports", "price": 29.99},
    {"id": "SPRT-002", "name": "Dumbbell Set 20lb", "category": "Sports", "price": 79.99},
    {"id": "SPRT-003", "name": "Camping Tent 4-Person", "category": "Sports", "price": 159.99},
    {"id": "SPRT-004", "name": "Bicycle Helmet", "category": "Sports", "price": 49.99},
    {"id": "SPRT-005", "name": "Water Bottle 32oz", "category": "Sports", "price": 19.99},
    {"id": "SPRT-006", "name": "Resistance Bands Set", "category": "Sports", "price": 24.99},
    {"id": "SPRT-007", "name": "Hiking Backpack", "category": "Sports", "price": 89.99},
    {"id": "SPRT-008", "name": "Tennis Racket", "category": "Sports", "price": 69.99},
    {"id": "SPRT-009", "name": "Sleeping Bag", "category": "Sports", "price": 59.99},
    {"id": "SPRT-010", "name": "Fitness Tracker Watch", "category": "Sports", "price": 99.99},
]


# User behavior profiles for realistic simulation
USER_BEHAVIOR_PROFILES = {
    "browser": {
        "purchase_probability": 0.05,
        "avg_products_viewed": 8,
        "avg_products_clicked": 3,
        "add_to_cart_probability": 0.15,
    },
    "researcher": {
        "purchase_probability": 0.20,
        "avg_products_viewed": 15,
        "avg_products_clicked": 8,
        "add_to_cart_probability": 0.30,
    },
    "buyer": {
        "purchase_probability": 0.70,
        "avg_products_viewed": 5,
        "avg_products_clicked": 2,
        "add_to_cart_probability": 0.80,
    },
}
