import logging
import os

import keyring
from astroquery.casda import Casda

logger = logging.getLogger(__name__)

CASDA_KEYRING_SERVICE = "astroquery:casda.csiro.au"

_casda_password: str | None = None


def _get_casda_password() -> str | None:
    p = os.environ.get("CASDA_PASSWORD")
    if p:
        return p
    try:
        from ....config import settings
        pw = getattr(settings, "CASDA_PASSWORD", None)
        return pw.get_secret_value() if pw else None
    except Exception:
        return None


class _EnvKeyringBackend(keyring.backend.KeyringBackend):
    priority = 1

    def get_password(self, service: str, username: str) -> str | None:
        if service == CASDA_KEYRING_SERVICE and _casda_password:
            return _casda_password
        return None

    def set_password(self, service: str, username: str, password: str) -> None:
        pass

    def delete_password(self, service: str, username: str) -> None:
        pass


def _ensure_env_password_in_keyring() -> None:
    global _casda_password
    _casda_password = _get_casda_password()
    if _casda_password:
        try:
            keyring.set_keyring(_EnvKeyringBackend())
        except Exception as e:
            logger.warning("event=casda_credentials_failed error=%s", e)


def init_casda_client(username: str) -> Casda:
    """
    1. CASDA_PASSWORD env var
    2. keyring
    """
    _ensure_env_password_in_keyring()
    if not _get_casda_password():
        logger.warning("event=casda_password_missing_using_keyring username=%s", username)

    casda = Casda()
    authenticated = casda.login(username=username)
    if authenticated is False:
        raise RuntimeError(
            "CASDA authentication failed. "
            "Ensure CASDA_USERNAME/CASDA_PASSWORD are set correctly (or provide keyring)."
        )
    logger.debug("event=casda_client_initialized username=%s", username)
    return casda
