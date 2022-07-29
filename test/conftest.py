from starlette.config import environ

environ["TOKEN_SERVER_SKEY"] = "c7351e82a2736bc721002bd480066a513eb3cb382dfdec71f18a13d10e47c1e60870e4643cd10f7a185e81ab5f5f7765616b7265665f5fc0"
environ["TOKEN_SERVER_DEFAULT_NB_TOKENS"] = "3"
environ["TOKEN_SERVER_COOKIE_SKEY"] = "secret"
environ["TOKEN_SERVER_COOKIE_NAME"] = "_session"
environ["TOKEN_SERVER_REDIS_URL"] = "redis://redis:6379"

environ["TOKEN_SERVER_OAUTH2_AUTHORIZE_URL"] = "/oauth/authorize"
environ["TOKEN_SERVER_OAUTH2_TOKEN_URL"] = "/oauth/token"
environ["TOKEN_SERVER_OAUTH2_USER_URL"] = "/api/me.json"
environ["TOKEN_SERVER_OAUTH2_SERVER_URL"] = "http://localhost:12346"
environ["TOKEN_SERVER_OAUTH2_CLIENT_ID"] = "oauth2_client_id"
environ["TOKEN_SERVER_OAUTH2_CLIENT_SECRET"] = "oauth2_client_secret"
