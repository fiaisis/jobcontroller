"""Helpers for working out what kind of reduction a job request message describes.

Most messages are fully described by their instrument, but nGEM (neutron-gamma) detector diagnostic
runs are not: they are queued under the instrument that owns the detector (IMAT today, other
instruments to follow) yet are reduced with plain Mantid (`mantid.simpleapi.LoadNGEM`) and read
their data from the nGEM share rather than the instrument's own data share. The nGEM check is
therefore always performed *before* the instrument check.
"""

from __future__ import annotations

import json
import os
from typing import Any

# Key that flags an nGEM run, must match what run-detection puts in additional_values (IMATFindImagesRule).
NGEM_KEY = os.environ.get("NGEM_KEY", "ngem")

# Special PV name that requests the nGEM share mount.
NGEM_SPECIAL_PV = os.environ.get("NGEM_SPECIAL_PV", "ngem")

_TRUTHY = ("true", "1", "yes")


def _as_dict(value: Any) -> dict[str, Any]:
    """
    Return a message entry as a dictionary, decoding it first if it arrived as a JSON string.
    :param value: The value taken from a message, may be a dict, a JSON string, or nothing at all.
    :return: The dictionary, or an empty dict if the value could not be interpreted as one.
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


def is_ngem_run(message: dict[str, Any]) -> bool:
    """
    Work out whether a message describes an nGEM detector run rather than the instruments usual reduction.

    Both ``additional_values["ngem"]`` (set by run-detection for autoreductions) and a top level
    ``ngem`` entry (used by rerun/simple messages) are accepted. Messages carrying neither, which is
    every rerun until the FIA API starts forwarding the flag, are not nGEM runs.

    :param message: The job request message received from the queue.
    :return: True if the message describes an nGEM run.
    """
    additional_values = _as_dict(message.get("additional_values"))
    flag = additional_values.get(NGEM_KEY, message.get(NGEM_KEY))
    return str(flag).strip().lower() in _TRUTHY
