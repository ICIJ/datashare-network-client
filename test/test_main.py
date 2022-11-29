from dsnetclient.main import Demo, extract_arg_from_docstring


def test_get_args_no_arg():
    assert extract_arg_from_docstring(Demo.do_peers.__doc__) == []


def test_get_args_one_arg():
    assert extract_arg_from_docstring(Demo.do_query.__doc__) == ['query']


def test_get_args_two_args():
    assert extract_arg_from_docstring(Demo.do_message.__doc__) == ['conversation_id', 'message']