from azfs.cli.constants import WELCOME_PROMPT
from click.testing import CliRunner
from azfs.cli import cmd


def test_cmd():
    result = CliRunner().invoke(cmd)
    # result.stdout
    assert result.stdout == f"{WELCOME_PROMPT}\n"
