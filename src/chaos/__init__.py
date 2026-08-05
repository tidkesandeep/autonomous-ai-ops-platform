"""Failure injection / chaos harness and demo reset."""

from src.chaos.injector import InjectionResult, inject, inject_all

__all__ = [
    "InjectionResult",
    "inject",
    "inject_all",
    "reset_demo",
    "reset_lakebase",
    "reset_ops_delta",
]


def __getattr__(name: str):
    if name in {"reset_demo", "reset_lakebase", "reset_ops_delta"}:
        from src.chaos import reset_demo as _reset

        return getattr(_reset, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
