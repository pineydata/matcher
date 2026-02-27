"""Tests for OOM warning helpers (device-driven when psutil available)."""

import warnings
from unittest.mock import patch

import polars as pl
import pytest

from matcher.oom import (
    _estimate_dataframe_bytes,
    _get_available_memory_bytes,
    _oom_warnings_enabled,
    warn_fuzzy_matrix_size,
    warn_matcher_input_size,
)


def test_oom_warnings_enabled_default():
    assert _oom_warnings_enabled() is True


def test_oom_warnings_disabled(monkeypatch):
    monkeypatch.setenv("MATCHER_OOM_WARN", "0")
    from matcher.oom import _oom_warnings_enabled as check
    assert check() is False


def test_estimate_dataframe_bytes():
    df = pl.DataFrame({"a": range(100), "b": [0.0] * 100})
    b = _estimate_dataframe_bytes(df)
    assert b > 0


def test_warn_matcher_input_size_small_no_warning():
    left = pl.DataFrame({"id": [1], "x": [1]})
    right = pl.DataFrame({"id": [2], "x": [2]})
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        warn_matcher_input_size(left, right)
    assert len(w) == 0


def test_warn_matcher_input_size_device_driven():
    """When device reports limited RAM, warn when inputs exceed fraction."""
    left = pl.DataFrame({"id": [1, 2], "x": [1, 2]})
    right = pl.DataFrame({"id": [3, 4], "x": [3, 4]})
    total = _estimate_dataframe_bytes(left) + _estimate_dataframe_bytes(right)
    # Mock available RAM so that total > 50% of available (e.g. avail = total => 100% > 50%)
    with patch("matcher.oom._get_available_memory_bytes", return_value=max(total, 1)):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            warn_matcher_input_size(left, right)
    # total is 50% of (total*2), so we should warn
    assert len(w) >= 1
    assert "available RAM" in str(w[0].message) or "device" in str(w[0].message).lower()


def test_warn_fuzzy_matrix_size_small_no_warning():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        warn_fuzzy_matrix_size(100, 100)
    assert len(w) == 0


def test_warn_fuzzy_matrix_size_device_driven():
    """When device reports limited RAM, warn when matrix exceeds fraction."""
    # 10k*10k*8 = 800 MB; mock 1 GB available so 800 MB > 40%
    with patch("matcher.oom._get_available_memory_bytes", return_value=1024**3):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            warn_fuzzy_matrix_size(10_000, 10_000)
    assert len(w) == 1
    assert "matcher" in str(w[0].message)
    assert "available RAM" in str(w[0].message) or "device" in str(w[0].message).lower()


def test_warn_fuzzy_matrix_size_fallback_when_no_device():
    """When device RAM unknown (no psutil), fall back to absolute threshold (500 MB)."""
    with patch("matcher.oom._get_available_memory_bytes", return_value=None):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            warn_fuzzy_matrix_size(10_000, 10_000)  # 800 MB > 500 MB
    assert len(w) == 1
    assert "psutil" in str(w[0].message) or "similarity matrix" in str(w[0].message)


def test_warn_fuzzy_matrix_size_respects_env(monkeypatch):
    monkeypatch.setenv("MATCHER_OOM_WARN", "0")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        warn_fuzzy_matrix_size(10_000, 10_000)
    assert len(w) == 0
