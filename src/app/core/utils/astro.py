"""Astronomy helpers."""
from typing import Any

import numpy as np


def to_python_value(value: Any) -> Any:
    """Convert NumPy/astropy table values to native Python types for JSON."""
    if hasattr(value, "item"):  # NumPy scalar (int32, float64, etc.)
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer, np.floating)):
        return float(value) if isinstance(value, np.floating) else int(value)
    if isinstance(value, (np.str_, np.bytes_)):
        return str(value)
    return value


def degrees_to_hms(degrees: float) -> tuple[int, int, float]:
    """Convert RA given in degrees to hours-minutes-seconds."""
    hours = degrees / 15.0  # Convert degrees to hours
    h = int(hours)  # Integer part of hours
    m = int((hours - h) * 60)  # Integer part of minutes
    s = (hours - h - m / 60.0) * 3600  # Seconds
    return h, m, s


def degrees_to_dms(degrees: float) -> tuple[int, int, float]:
    """Convert DEC given in degrees to degrees-minutes-seconds."""
    d = int(degrees)  # Integer part of degrees
    m = int(abs(degrees - d) * 60)  # Integer part of minutes
    s = (abs(degrees) - abs(d) - m / 60.0) * 3600  # Seconds
    return d, m, s
