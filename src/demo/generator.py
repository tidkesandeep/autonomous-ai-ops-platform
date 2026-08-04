"""Deterministic e-commerce data generator for the monitored `demo` catalog.

Generates customers, products, orders, events, and free-text reviews with a
fixed seed so demo resets are reproducible. Pure-Python / Faker output; callers
convert to Spark DataFrames when writing Delta tables.
"""

from __future__ import annotations

import hashlib
import random
from collections.abc import Iterator, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta

from faker import Faker

DEFAULT_SEED = 42

REVIEW_TEMPLATES = (
    "Great product, arrived quickly and works as advertised.",
    "Terrible quality — broke after two days. Very disappointed.",
    "Average experience. Shipping was fine but packaging was damaged.",
    "Absolutely love it! Would buy again without hesitation.",
    "Customer support was unhelpful when I asked about a refund.",
    "Solid value for the price. Not premium but gets the job done.",
    "The description did not match what showed up. Misleading listing.",
    "Fast delivery and the item looks exactly like the photos.",
)


@dataclass(frozen=True)
class Customer:
    customer_id: str
    email: str
    full_name: str
    country: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class Product:
    product_id: str
    sku: str
    name: str
    category: str
    price_usd: float
    is_active: bool
    updated_at: datetime


@dataclass(frozen=True)
class Order:
    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    order_ts: datetime
    status: str
    amount_usd: float


@dataclass(frozen=True)
class Event:
    event_id: str
    customer_id: str
    product_id: str | None
    event_type: str
    event_ts: datetime
    process_ts: datetime


@dataclass(frozen=True)
class Review:
    review_id: str
    customer_id: str
    product_id: str
    rating: int
    review_text: str
    review_ts: datetime


def _stable_id(prefix: str, n: int, seed: int) -> str:
    digest = hashlib.sha1(f"{seed}:{prefix}:{n}".encode()).hexdigest()[:12]
    return f"{prefix}_{digest}"


class EcommerceGenerator:
    """Config-driven generator for the five monitored pipelines' source data."""

    def __init__(
        self,
        seed: int = DEFAULT_SEED,
        n_customers: int = 1_000,
        n_products: int = 200,
        n_orders: int = 5_000,
        n_events: int = 20_000,
        n_reviews: int = 2_000,
        as_of: datetime | None = None,
    ) -> None:
        self.seed = seed
        self.n_customers = n_customers
        self.n_products = n_products
        self.n_orders = n_orders
        self.n_events = n_events
        self.n_reviews = n_reviews
        self.as_of = as_of or datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)
        self.fake = Faker()
        self.fake.seed_instance(seed)
        self.rng = random.Random(seed)

    def customers(self) -> list[Customer]:
        rows: list[Customer] = []
        for i in range(self.n_customers):
            created = self.as_of - timedelta(days=self.rng.randint(30, 900))
            rows.append(
                Customer(
                    customer_id=_stable_id("cust", i, self.seed),
                    email=self.fake.unique.email(),
                    full_name=self.fake.name(),
                    country=self.fake.country_code(),
                    created_at=created,
                    updated_at=created + timedelta(days=self.rng.randint(0, 60)),
                )
            )
        return rows

    def products(self) -> list[Product]:
        categories = ("electronics", "home", "apparel", "sports", "beauty")
        rows: list[Product] = []
        for i in range(self.n_products):
            rows.append(
                Product(
                    product_id=_stable_id("prod", i, self.seed),
                    sku=f"SKU-{i:05d}",
                    name=self.fake.catch_phrase(),
                    category=self.rng.choice(categories),
                    price_usd=round(self.rng.uniform(5.0, 499.0), 2),
                    is_active=self.rng.random() > 0.05,
                    updated_at=self.as_of - timedelta(days=self.rng.randint(0, 120)),
                )
            )
        return rows

    def orders(self, customers: Sequence[Customer], products: Sequence[Product]) -> list[Order]:
        statuses = ("placed", "shipped", "delivered", "cancelled", "returned")
        rows: list[Order] = []
        for i in range(self.n_orders):
            customer = self.rng.choice(customers)
            product = self.rng.choice(products)
            qty = self.rng.randint(1, 5)
            rows.append(
                Order(
                    order_id=_stable_id("ord", i, self.seed),
                    customer_id=customer.customer_id,
                    product_id=product.product_id,
                    quantity=qty,
                    order_ts=self.as_of - timedelta(hours=self.rng.randint(1, 24 * 60)),
                    status=self.rng.choice(statuses),
                    amount_usd=round(product.price_usd * qty, 2),
                )
            )
        return rows

    def events(self, customers: Sequence[Customer], products: Sequence[Product]) -> list[Event]:
        event_types = ("page_view", "add_to_cart", "checkout_start", "purchase", "search")
        rows: list[Event] = []
        for i in range(self.n_events):
            event_ts = self.as_of - timedelta(minutes=self.rng.randint(1, 60 * 24 * 45))
            # Most events processed within minutes; a few are intentionally late
            lag_minutes = 1 if self.rng.random() > 0.02 else self.rng.randint(24 * 60, 7 * 24 * 60)
            rows.append(
                Event(
                    event_id=_stable_id("evt", i, self.seed),
                    customer_id=self.rng.choice(customers).customer_id,
                    product_id=self.rng.choice(products).product_id if self.rng.random() > 0.1 else None,
                    event_type=self.rng.choice(event_types),
                    event_ts=event_ts,
                    process_ts=event_ts + timedelta(minutes=lag_minutes),
                )
            )
        return rows

    def reviews(self, customers: Sequence[Customer], products: Sequence[Product]) -> list[Review]:
        rows: list[Review] = []
        for i in range(self.n_reviews):
            rating = self.rng.randint(1, 5)
            base = self.rng.choice(REVIEW_TEMPLATES)
            extra = self.fake.sentence(nb_words=8)
            rows.append(
                Review(
                    review_id=_stable_id("rev", i, self.seed),
                    customer_id=self.rng.choice(customers).customer_id,
                    product_id=self.rng.choice(products).product_id,
                    rating=rating,
                    review_text=f"{base} {extra}",
                    review_ts=self.as_of - timedelta(days=self.rng.randint(0, 180)),
                )
            )
        return rows

    def generate_all(self) -> dict[str, list]:
        customers = self.customers()
        products = self.products()
        return {
            "customers": customers,
            "products": products,
            "orders": self.orders(customers, products),
            "events": self.events(customers, products),
            "reviews": self.reviews(customers, products),
        }


def records_as_dicts(rows: Sequence[object]) -> list[dict]:
    """Convert dataclass rows to plain dicts (datetimes preserved)."""
    return [asdict(r) for r in rows]  # type: ignore[arg-type]


def iter_batch(rows: Sequence[dict], batch_size: int) -> Iterator[list[dict]]:
    for i in range(0, len(rows), batch_size):
        yield list(rows[i : i + batch_size])


def business_date(ts: datetime) -> date:
    return ts.astimezone(UTC).date()
