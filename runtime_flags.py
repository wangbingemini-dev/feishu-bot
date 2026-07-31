import os
from collections.abc import Mapping


TRUTHY_VALUES = frozenset({"1", "true", "yes"})


def env_flag(
    name: str,
    *,
    default: bool = False,
    environ: Mapping[str, str] | None = None,
) -> bool:
    source = os.environ if environ is None else environ
    raw_value = source.get(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in TRUTHY_VALUES
