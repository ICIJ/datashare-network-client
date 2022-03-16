import os
import random
import string
from typing import Optional

import httpx
from authlib.oauth2.rfc6750 import BearerToken
from fastapi import HTTPException, FastAPI, Response, Depends
from fastapi import status, Form
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse
from yarl import URL

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="foo")

TOKEN_SERVER_HOST = URL(f"http://localhost:{os.getenv('TOKEN_SERVER_PORT', 12346)}")

TOKEN_MAP = dict()
USER_MAP = dict()
FAKE_USERS_DB = {
    "johndoe": {
        "username": "johndoe",
        "full_name": "John Doe",
        "email": "johndoe@example.com",
        "hashed_password": "fakehashedsecret",
        "disabled": False,
    },
    "alice": {
        "username": "alice",
        "full_name": "Alice Wonderson",
        "email": "alice@example.com",
        "hashed_password": "fakehashedsecret2",
        "disabled": True,
    },
}


def fake_hash_password(password: str):
    return "fakehashed" + password


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


class User(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = None


class UserInDB(User):
    hashed_password: str

    def __hash__(self):
        return hash(self.hashed_password)


def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)


def fake_decode_token(token):
    user = USER_MAP.get(token)
    return user


async def get_current_user(token: str = Depends(oauth2_scheme)):
    user = fake_decode_token(token)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


@app.get("/authorize")
async def authorize(request: Request):
    request.session['redirect_uri'] = request.query_params['redirect_uri']
    request.session['client_id'] = request.query_params['client_id']
    request.session['state'] = request.query_params['state']
    return Response(status_code=302, headers={"Location": "/signin"})


@app.get("/signin")
async def get_form():
    return HTMLResponse(content='<html><body><form method="post">'
                            '<input type="text" name="username"/>'
                            '<input type="password" name="password"/>'
                            '<input type="submit" value="Submit"/>'
                            '</form></body></html>')


@app.post("/signin")
async def signin(request: Request, form_data: OAuth2PasswordRequestForm = Depends()):
    user_dict = FAKE_USERS_DB.get(form_data.username)
    if not user_dict:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    user = UserInDB(**user_dict)
    hashed_password = fake_hash_password(form_data.password)
    if not hashed_password == user.hashed_password:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    response = Response(status_code=302)
    token = BearerToken(token_generator)(user, 'authenticate')
    TOKEN_MAP[token['access_token']] = token
    USER_MAP[token['access_token']] = user_dict
    response.headers['Location'] = f"http://127.0.0.1:12345/callback?code={token['access_token']}&state={request.session['state']}"
    return response


def token_generator(client, grant_type, user, scope):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=42))


@app.post("/token")
async def token(code: str = Form(...)):
    return TOKEN_MAP.pop(code)


@app.get("/users/me")
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user

URL_MAP = {
    "/api/v2/dstokens/publickey": URL("/publickey"),
    "/api/v2/dstokens/commitments": URL("/commitments"),
    "/api/v2/dstokens/pretokens": URL("/tokens"),
}


async def proxy_request(request: Request):
    async with httpx.AsyncClient() as client:
        body = await request.body()
        path = URL_MAP.get(request.url.path)
        url_join = str(URL.join(TOKEN_SERVER_HOST, path))
        response = await client.request(request.method, url_join, content=body)
        return Response(content=response.content, media_type=response.headers['Content-Type'])


@app.get("/api/v2/dstokens/publickey")
async def dstokens_publickey(request: Request, current_user: User = Depends(get_current_user)):
    return await proxy_request(request)


@app.get("/api/v2/dstokens/commitments")
async def dstokens_commitments(request: Request, current_user: User = Depends(get_current_user)):
    return await proxy_request(request)


@app.post("/api/v2/dstokens/pretokens")
async def dstokens_pretokens(request: Request, current_user: User = Depends(get_current_user)):
    return await proxy_request(request)


if __name__ == '__main__':
    import uvicorn
    import logging
    import sys
    # log requests to the identity provider
    logger = logging.getLogger('httpx')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    uvicorn.run(app, host='127.0.0.1', port=5001)
