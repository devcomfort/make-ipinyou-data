"""User-Agent parsing utilities.

Functions for extracting browser, OS, and device information from User-Agent strings.
These can be registered as DuckDB UDFs.

Original data is preserved as much as possible, with parsed information separated into columns.
Any further normalization can be performed by the data consumer.
"""

from __future__ import annotations

from ua_parser import user_agent_parser
from dataclasses import dataclass


@dataclass
class UserAgent:
    """Parsed User-Agent information.

    Attributes
    ----------
    user_agent : str | None
        Original User-Agent string
    browser_family : str | None
        Browser family (e.g., Chrome, Mobile Safari)
    browser_version : str | None
        Browser version (major.minor.patch)
    os_family : str | None
        OS family (e.g., Windows, iOS, Android)
    os_version : str | None
        OS version (major.minor.patch)
    device_family : str | None
        Device family (e.g., iPhone, Samsung SM-G973F)
    device_brand : str | None
        Device brand (e.g., Apple, Samsung)
    device_model : str | None
        Device model (e.g., iPhone, SM-G973F)
    """

    __slots__ = (
        "user_agent",
        "browser_family",
        "browser_version",
        "os_family",
        "os_version",
        "device_family",
        "device_brand",
        "device_model",
    )

    def __init__(self, user_agent: str | None) -> None:
        """Initialize UserAgent by parsing the User-Agent string.

        Parameters
        ----------
        user_agent : str | None
            User-Agent string to parse
        """
        self.user_agent = user_agent

        parsed = user_agent_parser.Parse(user_agent) if user_agent else {}

        # Browser info
        user_agent_info = parsed.get("user_agent", {})
        self.browser_family: str | None = user_agent_info.get("family")
        self.browser_version: str | None = self._format_version(
            user_agent_info.get("major"),
            user_agent_info.get("minor"),
            user_agent_info.get("patch"),
        )

        # OS info
        os_info = parsed.get("os", {})
        self.os_family: str | None = os_info.get("family")
        self.os_version: str | None = self._format_version(
            os_info.get("major"),
            os_info.get("minor"),
            os_info.get("patch"),
        )

        # Device info
        device_info = parsed.get("device", {})
        self.device_family: str | None = device_info.get("family")
        self.device_brand: str | None = device_info.get("brand")
        self.device_model: str | None = device_info.get("model")

    @staticmethod
    def _format_version(
        major: str | None,
        minor: str | None,
        patch: str | None,
    ) -> str | None:
        """Format version information as 'major.minor.patch'.

        Parameters
        ----------
        major : str | None
            Major version
        minor : str | None
            Minor version
        patch : str | None
            Patch version

        Returns
        -------
        str | None
            Formatted version string, or None if no information is available
        """
        parts = []
        if major:
            parts.append(major)
            if minor:
                parts.append(minor)
                if patch:
                    parts.append(patch)
        return ".".join(parts) if parts else None
