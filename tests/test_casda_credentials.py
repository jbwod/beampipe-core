from unittest.mock import patch


def test_init_casda_client_sets_auth_from_env(monkeypatch):
    monkeypatch.setenv("CASDA_PASSWORD", "dummy-password")

    with patch("app.core.archive.adapters.casda.credentials.Casda") as MockCasda:
        instance = MockCasda.return_value
        instance.login.return_value = True
        instance._auth = ("dummy-user", "dummy-password")

        from app.core.archive.adapters.casda.credentials import init_casda_client

        casda = init_casda_client("dummy-user")
        assert hasattr(casda, "_auth")
        instance.login.assert_called_once_with(username="dummy-user")