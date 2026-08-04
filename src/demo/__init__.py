"""Monitored e-commerce platform (demo catalog) package."""

from src.demo.generator import EcommerceGenerator, records_as_dicts
from src.demo.pipelines import run_medallion

__all__ = ["EcommerceGenerator", "records_as_dicts", "run_medallion"]
