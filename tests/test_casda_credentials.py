def test_init_casda_client_sets_auth_from_env(monkeypatch):
    monkeypatch.setenv("CASDA_PASSWORD", "dummy-password")

    from astroquery.casda import Casda as _Casda

    def _fake_login(self, *, username=None, store_password=False, reenter_password=False):
        self._auth = (username, "dummy-password")
        return True

    monkeypatch.setattr(_Casda, "login", _fake_login, raising=True)

    from app.core.archive.adapters.casda.credentials import init_casda_client

    casda = init_casda_client("dummy-user")
    assert hasattr(casda, "_auth")