"""Astronomy helpers."""
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
