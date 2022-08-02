import os
import random
import string
from typing import Optional, Dict, Union

from authlib.oauth2.rfc6750 import BearerToken
from starlette import status
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, Response, JSONResponse
from starlette.routing import Route

TOKEN_SERVER_HOST = f"http://localhost:{os.getenv('TOKEN_SERVER_PORT', 12346)}"
TOKEN_SERVER_NBTOKENS = os.getenv('TOKEN_SERVER_NBTOKENS', 3)

TOKEN_MAP = dict()
USER_MAP = dict()


class User:
    def __init__(self, username: str, hashed_password: str,
                 email: Optional[str] = None, full_name: Optional[str] = None,
                 disabled: bool = False):
        self.username = username
        self.email = email
        self.full_name = full_name
        self.disabled = disabled
        self.hashed_password = hashed_password

    def __hash__(self):
        return hash(self.hashed_password)

    def view(self) -> Dict[str, Union[str, bool]]:
        return {
            "username": self.username,
            "full_name": self.full_name,
            "email": self.email,
            "disabled": self.disabled
        }


FAKE_USERS_DB = {
    "johndoe": User(
        username="johndoe",
        full_name="John Doe",
        email="johndoe@example.com",
        hashed_password="fakehashedsecret",
        disabled=False,
    ),
    "alice": User(
        username="alice",
        full_name="Alice Wonderson",
        email="alice@example.com",
        hashed_password="fakehashedsecret2",
        disabled=True,
    ),
}


def fake_hash_password(password: str):
    return "fakehashed" + password


def get_user(db, username: str):
    if username in db:
        return db[username]


def fake_decode_token(token: str) -> User:
    return USER_MAP.get(token)


def get_current_user(token: str) -> User:
    user = fake_decode_token(token)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


async def authorize(request: Request):
    request.session['redirect_uri'] = request.query_params['redirect_uri']
    request.session['client_id'] = request.query_params['client_id']
    request.session['state'] = request.query_params['state']
    return Response(status_code=302, headers={"Location": "/signin"})


async def get_form(_request: Request):
    return HTMLResponse(content='<html><body><form method="post">'
                                '<input type="text" name="username"/>'
                                '<input type="password" name="password"/>'
                                '<input type="submit" value="Submit"/>'
                                '</form></body></html>')


async def signin(request: Request):
    form_data = await request.form()
    user = FAKE_USERS_DB.get(form_data.get('username'))
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    hashed_password = fake_hash_password(form_data.get('password'))
    if not hashed_password == user.hashed_password:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    response = Response(status_code=302)
    token = BearerToken(token_generator)(user, 'authenticate')
    TOKEN_MAP[token['access_token']] = token
    USER_MAP[token['access_token']] = user
    response.headers['Location'] = f"http://localhost:12345/auth/callback?code={token['access_token']}&state={request.session['state']}"
    return response


def token_generator(client, grant_type, user, scope):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=42))


async def token(request: Request):
    form_data = await request.form()
    content = TOKEN_MAP.pop(form_data.get('code'))
    return JSONResponse(content)


async def read_users_me(request: Request):
    authorization_header = request.headers.get('Authorization')
    token = authorization_header.replace('Bearer ', '')
    return JSONResponse(get_current_user(token).view())

def setup_app():
    routes = [
        Route('/oauth/authorize', authorize, methods=['GET']),
        Route('/oauth/token', token, methods=['POST']),
        Route('/signin', signin, methods=['POST']),
        Route('/signin', get_form, methods=['GET']),
        Route('/api/me.json', read_users_me, methods=['GET']),
    ]

    app = Starlette(routes=routes, middleware=[Middleware(SessionMiddleware, secret_key="foo")])
    return app

app = setup_app()

if __name__ == '__main__':
    import uvicorn
    import logging
    import sys
    logger = logging.getLogger('httpx')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    uvicorn.run(app, host='127.0.0.1', port=5001)
