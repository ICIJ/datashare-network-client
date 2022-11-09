import re


def tokenize_with_double_quotes(string: str):
    return [s1 if not s2 else s2 for s1, s2 in re.findall(r'(\w+|"(.*?)")', string)]