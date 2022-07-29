from typing import Tuple

from bs4 import BeautifulSoup


def bs_parser(html: bytes, username: str, password: str) -> Tuple[str, dict]:
    parameters = dict()
    soup = BeautifulSoup(html)
    forms = soup.find_all("form")
    form_url = forms[0].attrs.get('action')
    inputs = forms[0].find_all("input")

    for input in inputs:
        if input.attrs["type"] == "hidden" and 'name' in input.attrs and 'value' in input.attrs:
            parameters[input.attrs['name']] = input.attrs['value']
        elif input.attrs["type"] == "password":
            parameters[input.attrs["name"]] = password
        elif input.attrs["type"] == "text" and input.attrs['name'].startswith('user'):
            parameters[input.attrs["name"]] = username
    return form_url, parameters


if __name__ == "__main__":
    from pprint import pprint
    html = b"""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" class="login-pf">

<head>
    <meta charset="utf-8">
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
    <meta name="robots" content="noindex, nofollow">

            <meta name="viewport" content="width=device-width,initial-scale=1"/>
    <title>Log in to Keycloak</title>
    <link rel="icon" href="/auth/resources/ktggf/login/keycloak/img/favicon.ico" />
            <link href="/auth/resources/ktggf/common/keycloak/node_modules/patternfly/dist/css/patternfly.min.css" rel="stylesheet" />
            <link href="/auth/resources/ktggf/common/keycloak/node_modules/patternfly/dist/css/patternfly-additions.min.css" rel="stylesheet" />
            <link href="/auth/resources/ktggf/common/keycloak/lib/zocial/zocial.css" rel="stylesheet" />
            <link href="/auth/resources/ktggf/login/keycloak/css/login.css" rel="stylesheet" />
</head>

<body class="">
  <div class="login-pf-page">
    <div id="kc-header" class="login-pf-page-header">
      <div id="kc-header-wrapper" class=""><div class="kc-logo-text"><span>Keycloak</span></div></div>
    </div>
    <div class="card-pf ">
      <header class="login-pf-header">
                <h1 id="kc-page-title">        Log In

</h1>
      </header>
      <div id="kc-content">
        <div id="kc-content-wrapper">


    <div id="kc-form" >
      <div id="kc-form-wrapper" >
            <form id="kc-form-login" onsubmit="login.disabled = true; return true;" action="http://localhost:9080/auth/realms/master/login-actions/authenticate?session_code=2ORCcwqRJP5Ol6u4NYHoxE5WR2DQL5lXJAdBX89I5go&amp;execution=30a16294-eede-449b-be72-e5390722a39d&amp;client_id=security-admin-console&amp;tab_id=W74dMOxVClQ" method="post">
                <div class="form-group">
                    <label for="username" class="control-label">Username or email</label>

                        <input tabindex="1" id="username" class="form-control" name="username" value=""  type="text" autofocus autocomplete="off" />
                </div>

                <div class="form-group">
                    <label for="password" class="control-label">Password</label>
                    <input tabindex="2" id="password" class="form-control" name="password" type="password" autocomplete="off" />
                </div>

                <div class="form-group login-pf-settings">
                    <div id="kc-form-options">
                        </div>
                        <div class="">
                        </div>

                  </div>

                  <div id="kc-form-buttons" class="form-group">
                      <input type="hidden" id="id-hidden-input" name="credentialId" />
                      <input tabindex="4" class="btn btn-primary btn-block btn-lg" name="login" id="kc-login" type="submit" value="Log In"/>
                  </div>
            </form>
        </div>
      </div>



        </div>
      </div>

    </div>
  </div>
</body>
</html>
"""
    url, params = bs_parser(html, "foo", "bar")
    print(url)
    pprint(params)