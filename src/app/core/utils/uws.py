"""Utilities for parsing IVOA Universal Worker Service (UWS) job results."""

import os
import re
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from urllib.parse import parse_qs, unquote, urlparse

UWS_NS = "http://www.ivoa.net/xml/UWS/v1.0"
XLINK_NS = "http://www.w3.org/1999/xlink"


def iter_uws_results(xml_text: str) -> Iterator[tuple[str, str]]:
    """Yield (result_id, url) for each UWS result element.
    """
    root = ET.fromstring(xml_text)
    for elem in root.findall(f".//{{{UWS_NS}}}result"):
        result_id = elem.attrib.get("id", "")
        href = elem.attrib.get(f"{{{XLINK_NS}}}href", "")
        if href:
            yield result_id, unquote(href)


def extract_filename_from_url(url: str) -> str | None:
    """Extract filename from URL (response-content-disposition or path).
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    for val in qs.get("response-content-disposition", []):
        m = re.search(r'filename="?([^";]+)"?', val)
        if m:
            return m.group(1)
    fn = os.path.basename(parsed.path.rstrip("/"))
    return fn if fn else None
