"""Path, URL, and filename guards.

Local file access is limited to configured roots. External URLs must match the
host allowlist, use http or https, and survive a DNS resolution check that
rejects cloud metadata addresses. Private and loopback hosts are allowed only
when `allow_private_source_hosts` is true, which is the local-development default.
"""

from __future__ import annotations

import ipaddress
import socket
from pathlib import Path
from urllib.parse import urljoin, urlparse

from app.core.config import Settings
from app.core.exceptions import UnsafePathError, UnsafeURLError

_ALWAYS_BLOCKED = {
    ipaddress.ip_address("169.254.169.254"),
    ipaddress.ip_address("169.254.170.2"),
}
_UNSAFE_SCHEMES = {"file", "ftp", "gopher", "data", "javascript", "ws", "wss"}


def sanitize_filename(name: str) -> str:
    """Drop directories and characters that are unsafe on common filesystems."""
    base = Path(name).name.strip().replace("\x00", "")
    cleaned = []
    for char in base:
        if char.isalnum() or char in {".", "_", "-"}:
            cleaned.append(char)
        else:
            cleaned.append("_")
    result = "".join(cleaned).strip("._")[:180]
    return result or "document"


def resolve_allowed_path(path: str | Path, settings: Settings) -> Path:
    """Resolve `path` and require it to stay inside an allowed root."""
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    resolved = candidate.resolve()
    roots = list(settings.allowed_file_root_list)
    data_root = settings.data_path
    if data_root not in roots:
        roots.append(data_root)
    for root in roots:
        if resolved == root or root in resolved.parents:
            return resolved
    raise UnsafePathError(
        "Path is outside the configured file roots",
        details={"path": str(resolved)},
    )


def _host_allowed(host: str, settings: Settings) -> bool:
    allowed = settings.allowed_source_host_set
    lowered = host.lower().rstrip(".")
    if lowered in allowed:
        return True
    return any(lowered == item or lowered.endswith("." + item) for item in allowed)


def _ip_permitted(ip: ipaddress.IPv4Address | ipaddress.IPv6Address, settings: Settings) -> None:
    # IPv6 loopback (::1) is flagged is_reserved. Local sample APIs are allowed
    # only when ALLOW_PRIVATE_SOURCE_HOSTS is enabled, which the next check enforces.
    reserved = ip.is_reserved and not ip.is_loopback
    if (
        ip in _ALWAYS_BLOCKED
        or ip.is_link_local
        or ip.is_multicast
        or reserved
        or ip.is_unspecified
    ):
        raise UnsafeURLError("URL resolves to a blocked address")
    if not settings.allow_private_source_hosts and (ip.is_private or ip.is_loopback):
        raise UnsafeURLError("URL resolves to a private or loopback address")


def assert_url_allowed(url: str, settings: Settings) -> str:
    """Validate a source URL before any request is sent.

    Returns the normalized URL. Raises UnsafeURLError when the URL is not eligible.
    """
    parsed = urlparse(url.strip())
    if parsed.scheme.lower() not in {"http", "https"}:
        raise UnsafeURLError("Only http and https source URLs are allowed")
    if parsed.scheme.lower() in _UNSAFE_SCHEMES:
        raise UnsafeURLError("URL scheme is not allowed")
    host = parsed.hostname
    if not host or parsed.username or parsed.password:
        raise UnsafeURLError("Source URLs must not embed credentials")
    if not _host_allowed(host, settings):
        raise UnsafeURLError("Source host is not in ALLOWED_SOURCE_HOSTS")
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    addresses: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
    if literal is not None:
        addresses.append(literal)
    else:
        try:
            infos = socket.getaddrinfo(
                host, parsed.port or (443 if parsed.scheme == "https" else 80)
            )
        except socket.gaierror as exc:
            raise UnsafeURLError("Source host could not be resolved") from exc
        for info in infos:
            ip_text = info[4][0]
            addresses.append(ipaddress.ip_address(ip_text))
    if not addresses:
        raise UnsafeURLError("Source host did not resolve to an address")
    for ip in addresses:
        _ip_permitted(ip, settings)
    return url


def next_redirect_url(current_url: str, location: str, settings: Settings) -> str:
    target = urljoin(current_url, location)
    return assert_url_allowed(target, settings)
