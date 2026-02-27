"""OOM (out-of-memory) risk warnings based on data size and device RAM.

Warnings are driven by the **device** when possible: we check available RAM (via
optional ``psutil``) and warn when estimated usage would exceed a fraction of it.
If device RAM cannot be read (e.g. psutil not installed), we fall back to absolute
size thresholds.

- **Matcher**: when device available: warn if left+right > 50% of available RAM;
  otherwise warn if total > 2 GB.
- **Deduplicator**: when device available: warn if ~2× source size > 50% of available;
  otherwise warn if source > 1 GB.
- **FuzzyMatcher**: when device available: warn if similarity matrix (N×M×8 bytes) > 40%
  of available RAM; otherwise warn if matrix > 500 MB.

To disable warnings: set ``MATCHER_OOM_WARN=0``. Device RAM is read via ``psutil`` (a
dependency). Absolute thresholds are used when device RAM cannot be determined (e.g.
restricted or virtualized environments).
"""

from __future__ import annotations

import os
import warnings
from typing import Optional

from polars import DataFrame


def _oom_warnings_enabled() -> bool:
    """True if OOM warnings should be emitted (env MATCHER_OOM_WARN != '0')."""
    return os.environ.get("MATCHER_OOM_WARN", "1") != "0"


def _get_available_memory_bytes() -> Optional[int]:
    """Approximate free/available RAM in bytes, or None if unknown (e.g. no psutil)."""
    try:
        import psutil
        mem = psutil.virtual_memory()
        return mem.available
    except ImportError:
        return None


def _estimate_dataframe_bytes(df: DataFrame) -> int:
    """Best-effort estimate of DataFrame memory in bytes."""
    try:
        return df.estimated_size()
    except AttributeError:
        # Fallback: rough upper bound (rows * cols * 8 bytes)
        return df.height * max(df.width, 1) * 8


def _format_mb(b: int) -> str:
    return f"{b / (1024 * 1024):.0f} MB"


# Fraction of available RAM above which we warn (device-driven)
_MATCHER_RAM_FRACTION = 0.5
_DEDUP_RAM_FRACTION = 0.5
_FUZZY_RAM_FRACTION = 0.4
# Absolute fallbacks when device RAM is unknown (bytes)
_MATCHER_ABSOLUTE_THRESHOLD = 2 * 1024 * 1024 * 1024   # 2 GB
_DEDUP_ABSOLUTE_THRESHOLD = 1 * 1024 * 1024 * 1024    # 1 GB
_FUZZY_ABSOLUTE_THRESHOLD = 500 * 1024 * 1024         # 500 MB


def warn_matcher_input_size(left: DataFrame, right: DataFrame) -> None:
    """Warn when left + right estimated size is large vs device available RAM (or absolute threshold).

    Call from Matcher.__init__.
    """
    if not _oom_warnings_enabled():
        return
    left_b = _estimate_dataframe_bytes(left)
    right_b = _estimate_dataframe_bytes(right)
    total = left_b + right_b
    avail = _get_available_memory_bytes()
    if avail is not None:
        # Device check: warn if inputs would use more than fraction of available RAM
        if total > _MATCHER_RAM_FRACTION * avail:
            warnings.warn(
                f"matcher: Inputs (left ~{_format_mb(left_b)}, right ~{_format_mb(right_b)}; "
                f"total ~{_format_mb(total)}) are ~{100 * total / avail:.0f}% of this device's "
                f"available RAM (~{_format_mb(avail)}). OOM risk. Consider BatchedMatcher or blocking_key.",
                UserWarning,
                stacklevel=2,
            )
    elif total > _MATCHER_ABSOLUTE_THRESHOLD:
        warnings.warn(
            f"matcher: Large inputs (total ~{_format_mb(total)}). OOM risk on memory-limited devices. "
            "Install psutil for device-aware warnings. Consider BatchedMatcher or blocking_key.",
            UserWarning,
            stacklevel=2,
        )


def warn_fuzzy_matrix_size(left_height: int, right_height: int) -> None:
    """Warn when fuzzy similarity matrix (N×M floats) would be large vs device RAM (or absolute threshold).

    Call from FuzzyMatcher.match() when about to build the matrix.
    """
    if not _oom_warnings_enabled():
        return
    matrix_bytes = left_height * right_height * 8  # float64
    avail = _get_available_memory_bytes()
    if avail is not None:
        if matrix_bytes > _FUZZY_RAM_FRACTION * avail:
            warnings.warn(
                f"matcher: Fuzzy matching will build a {left_height:,}×{right_height:,} similarity matrix "
                f"(~{_format_mb(matrix_bytes)}), ~{100 * matrix_bytes / avail:.0f}% of this device's "
                f"available RAM (~{_format_mb(avail)}). OOM risk. Use blocking_key to limit comparisons.",
                UserWarning,
                stacklevel=2,
            )
    elif matrix_bytes > _FUZZY_ABSOLUTE_THRESHOLD:
        warnings.warn(
            f"matcher: Fuzzy matching will build a {left_height:,}×{right_height:,} similarity matrix "
            f"(~{_format_mb(matrix_bytes)}). OOM risk on memory-limited devices. "
            "Install psutil for device-aware warnings. Use blocking_key to limit comparisons.",
            UserWarning,
            stacklevel=2,
        )


def warn_deduplicator_source_size(source: DataFrame) -> None:
    """Warn when Deduplicator would hold ~2× source vs device available RAM (or absolute threshold)."""
    if not _oom_warnings_enabled():
        return
    size_b = _estimate_dataframe_bytes(source)
    approx_hold = 2 * size_b  # source + clone
    avail = _get_available_memory_bytes()
    if avail is not None:
        if approx_hold > _DEDUP_RAM_FRACTION * avail:
            warnings.warn(
                f"matcher: Deduplicator source is ~{_format_mb(size_b)}; dedup holds ~2× that "
                f"(~{_format_mb(approx_hold)}), ~{100 * approx_hold / avail:.0f}% of this device's "
                f"available RAM (~{_format_mb(avail)}). OOM risk. Consider BatchedMatcher.",
                UserWarning,
                stacklevel=2,
            )
    elif size_b > _DEDUP_ABSOLUTE_THRESHOLD:
        warnings.warn(
            f"matcher: Deduplicator source is ~{_format_mb(size_b)} (~2× held). "
            "OOM risk on memory-limited devices. Install psutil for device-aware warnings. "
            "Consider BatchedMatcher.",
            UserWarning,
            stacklevel=2,
        )
