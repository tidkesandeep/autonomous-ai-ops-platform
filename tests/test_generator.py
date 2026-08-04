"""Unit tests for the e-commerce data generator (no Spark required)."""

from __future__ import annotations

from src.demo.generator import EcommerceGenerator, records_as_dicts


def test_generator_is_deterministic():
    a = EcommerceGenerator(seed=7, n_customers=50, n_products=10, n_orders=100, n_events=200, n_reviews=40)
    b = EcommerceGenerator(seed=7, n_customers=50, n_products=10, n_orders=100, n_events=200, n_reviews=40)
    assert records_as_dicts(a.customers()) == records_as_dicts(b.customers())
    assert records_as_dicts(a.products()) == records_as_dicts(b.products())


def test_generator_row_counts():
    gen = EcommerceGenerator(
        seed=1,
        n_customers=20,
        n_products=5,
        n_orders=40,
        n_events=80,
        n_reviews=15,
    )
    data = gen.generate_all()
    assert len(data["customers"]) == 20
    assert len(data["products"]) == 5
    assert len(data["orders"]) == 40
    assert len(data["events"]) == 80
    assert len(data["reviews"]) == 15


def test_orders_reference_existing_keys():
    gen = EcommerceGenerator(seed=3, n_customers=10, n_products=4, n_orders=25, n_events=10, n_reviews=5)
    data = gen.generate_all()
    customer_ids = {c.customer_id for c in data["customers"]}
    product_ids = {p.product_id for p in data["products"]}
    for order in data["orders"]:
        assert order.customer_id in customer_ids
        assert order.product_id in product_ids


def test_reviews_have_free_text():
    gen = EcommerceGenerator(seed=9, n_customers=5, n_products=3, n_orders=5, n_events=5, n_reviews=8)
    for review in gen.reviews(gen.customers(), gen.products()):
        assert len(review.review_text) > 20
        assert 1 <= review.rating <= 5


def test_failure_type_priority_ordering():
    from src.common.constants import FAILURE_TYPE_PRIORITY

    ordered = sorted(FAILURE_TYPE_PRIORITY, key=FAILURE_TYPE_PRIORITY.get)
    assert ordered[0] == "job_crash"
    assert ordered[-1] == "late_data"
