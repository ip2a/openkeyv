"""Beartype configuration and decorators for runtime type checking.

This module provides decorators for controlling beartype's runtime type checking
behavior. Use `bear_enforce` when you want type violations to raise exceptions
rather than warnings.
"""

import os
from collections.abc import Callable

from beartype import BeartypeConf, BeartypeStrategy, beartype
from typing_extensions import ParamSpec, TypeVar

P = ParamSpec(name="P")
R = TypeVar(name="R")

_disable_beartype = os.environ.get("OPENKEYV_DISABLE_BEARTYPE", "false").lower() in ("true", "1", "yes")

no_bear_type_check_conf = BeartypeConf(strategy=BeartypeStrategy.O0)
no_bear_type = beartype(conf=no_bear_type_check_conf)

enforce_bear_type_conf = BeartypeConf(strategy=BeartypeStrategy.O1, violation_type=TypeError)
enforce_bear_type = beartype(conf=enforce_bear_type_conf)


def bear_enforce(func: Callable[P, R]) -> Callable[P, R]:
    """Enforce beartype with exceptions instead of warnings."""
    if _disable_beartype:
        return func
    return enforce_bear_type(func)


def no_bear_type_check(func: Callable[P, R]) -> Callable[P, R]:
    """Disable beartype checking for a function."""
    if _disable_beartype:
        return func
    return no_bear_type(func)
