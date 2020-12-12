from azfs.cli.constants import WELCOME_PROMPT, MOCK_FUNCTION
from click.testing import CliRunner
from azfs.cli import cmd


def test_cli_output():
    result = CliRunner().invoke(cmd)
    # result.stdout
    assert result.stdout == f"{WELCOME_PROMPT}\n"
