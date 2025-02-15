from pipeline.utils.ver import get_pipe_ver
import re


def test_get_pipe_ver():
    match = re.match(r"[0-9]\.[0-9]\.[0-9]", get_pipe_ver())
    assert match
