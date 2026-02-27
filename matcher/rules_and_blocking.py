"""Rule normalization and blocking for match_on and block_on.

Provides normalize_fields() (for match_on or block_on) and paired_blocks_by_key().
Used by Matcher and BatchedMatcher so rule format and blocking behavior stay in one place.
"""

from __future__ import annotations

from typing import Union, Optional

import polars as pl
from polars import DataFrame


def normalize_fields(
    value: Optional[Union[str, list]],
    *,
    allow_none: bool = False,
    single_rule_only: bool = False,
    param: str = "value",
) -> Optional[list[str]]:
    """Normalize match_on or block_on to a list of field names (or None when block_on is omitted).

    Args:
        value: str (one field), list[str] (multiple fields), or for single_rule_only
            list of one list (e.g. [["first_name", "last_name"]]).
        allow_none: If True, None is returned as-is. Use when no blocking (block_on omitted).
        single_rule_only: If True, accept only a single rule (reject list of multiple lists).
            Use for match_on; use .refine(match_on=...) for cascading.
        param: Name for error messages (e.g. "match_on", "block_on").

    Returns:
        list[str] of column names, or None if allow_none and value is None.
    """
    if value is None:
        if allow_none:
            return None
        raise ValueError(f"{param} must be provided")
    if isinstance(value, str):
        return [value]
    if not isinstance(value, list) or len(value) == 0:
        raise ValueError(f"{param} must be a non-empty string or list of field names")
    if all(isinstance(item, str) for item in value):
        return list(value)
    if single_rule_only and len(value) == 1 and isinstance(value[0], list):
        if len(value[0]) == 0:
            raise ValueError(f"{param} must contain at least one field name")
        return list(value[0])
    if single_rule_only:
        raise ValueError(
            "Only a single rule is allowed per match(). "
            "For cascading (next rule on unmatched), use .refine(match_on=...) after match()."
        )
    raise ValueError(
        f"{param} must be a column name (str) or list of column names (list[str])"
    )


def paired_blocks_by_key(
    left_df: DataFrame,
    right_df: DataFrame,
    keys: list[str],
) -> list[tuple[DataFrame, DataFrame]]:
    """Return (left_block, right_block) for each common blocking key value(s).

    keys: list of column names; a block = same value for all. Nulls: rows where all
    blocking columns are null form one block.
    """
    left_vals = left_df.select(keys).unique()
    right_vals = right_df.select(keys).unique()
    common = left_vals.join(right_vals, on=keys, how="inner")
    pairs = []
    for row in common.iter_rows(named=True):
        block_vals = [row[k] for k in keys]
        if all(v is None for v in block_vals):
            left_b = left_df.filter(
                pl.all_horizontal(pl.col(k).is_null() for k in keys)
            )
            right_b = right_df.filter(
                pl.all_horizontal(pl.col(k).is_null() for k in keys)
            )
        else:
            pred_left = pl.all_horizontal(
                pl.col(k) == v for k, v in zip(keys, block_vals)
            )
            pred_right = pl.all_horizontal(
                pl.col(k) == v for k, v in zip(keys, block_vals)
            )
            left_b = left_df.filter(pred_left)
            right_b = right_df.filter(pred_right)
        pairs.append((left_b, right_b))
    null_pred = pl.all_horizontal(pl.col(k).is_null() for k in keys)
    if left_df.filter(null_pred).height > 0 and right_df.filter(null_pred).height > 0:
        left_b = left_df.filter(null_pred)
        right_b = right_df.filter(null_pred)
        pairs.append((left_b, right_b))
    return pairs
