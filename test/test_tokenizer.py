from dsnetclient.tokenizer import tokenize_with_double_quotes


def test_tokenizer_hello_world():
    assert ['hello', 'world'] == tokenize_with_double_quotes('hello world')


def test_tokenizer_with_one_term_between_double_quotes():
    assert tokenize_with_double_quotes('Donald Trump "Donald Trump"') == ['Donald', 'Trump', 'Donald Trump']


def test_tokenizer_with_two_terms_between_double_quotes():
    assert tokenize_with_double_quotes('"foo bar" baz "qux fred" thud') == ['foo bar', 'baz', 'qux fred', 'thud']