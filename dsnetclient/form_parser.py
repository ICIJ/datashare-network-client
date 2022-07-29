from typing import Tuple

from bs4 import BeautifulSoup


def bs_parser(html: bytes, username: str, password: str) -> Tuple[str, dict]:
    parameters = dict()
    soup = BeautifulSoup(html, features="html.parser")
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

