from inspect import signature
import os
from typing import List

import click
import azfs
from .factory import CliFactory
from .constants import WELCOME_PROMPT, MOCK_FUNCTION


@click.group(invoke_without_command=True)
@click.option('--target-file-dir')
@click.pass_context
def cmd(ctx, target_file_dir: str):
    """
    root command for the `azfs`.
    Currently, the sub-command below is supported:

    * decorator

    """
    if target_file_dir is None:
        target_file_dir = os.getcwd()
    if ctx.invoked_subcommand is None:
        click.echo(WELCOME_PROMPT)
    ctx.obj['factory'] = CliFactory(target_file_dir=target_file_dir)


def _read_az_file_client_content() -> List[str]:
    az_file_client_path = f"{azfs.__file__.rsplit('/', 1)[0]}/az_file_client.py"

    with open(az_file_client_path, "r") as f:
        az_file_client_content = f.readlines()

    main_file_index = len(az_file_client_content)
    for main_file_index, content in enumerate(az_file_client_content):
        if "# end of the main file" in content:
            break

    az_file_client_content = az_file_client_content[:main_file_index+1]
    return az_file_client_content


def _write_az_file_client_content(az_file_client_content: List[str]):
    az_file_client_path = f"{azfs.__file__.rsplit('/', 1)[0]}/az_file_client.py"
    with open(az_file_client_path, "w") as f:
        f.writelines(az_file_client_content)


def _load_functions(export_decorator) -> (int, List[str]):
    new_lines = []
    append_functions = len(export_decorator.functions)

    for f in export_decorator.functions:
        function_name = f['register_as']
        sig = signature(f['function'])
        ideal_sig = str(sig)
        if "()" in ideal_sig:
            ideal_sig = ideal_sig.replace(")", "**kwargs)")
        else:
            ideal_sig = ideal_sig.replace(")", ", **kwargs)")

        ideal_sig = ideal_sig.replace("pandas.core.frame.DataFrame", "pd.DataFrame")
        new_mock_function: str = MOCK_FUNCTION % (function_name, ideal_sig)

        new_mock_function_content = [f"{s}\n" for s in new_mock_function.split("\n")]
        new_lines.extend(new_mock_function_content)
        click.echo(f"    * {function_name}{ideal_sig}")
    return append_functions, new_lines


@cmd.command("decorator")
@click.option("-n", "--target-file-name", multiple=True)
@click.pass_context
def decorator(ctx, target_file_name: list):
    """
    add decorated functions to the file `az_file_client.py`

    """
    cli_factory: CliFactory = ctx.obj['factory']
    if len(target_file_name) == 0:
        target_file_name = ["__init__"]
    # set initial state
    append_functions = 0
    append_content = []

    # append target functions
    for file_name in target_file_name:
        _export_decorator: azfs.az_file_client.ExportDecorator = cli_factory.load_export_decorator(file_name)
        newly_added, tmp_append_content = _load_functions(_export_decorator)
        append_functions += newly_added
        append_content.extend(tmp_append_content)

    # read `az_file_client.py`
    az_file_client_content = _read_az_file_client_content()

    # append newly added content
    az_file_client_content.extend(append_content)

    # over-write `az_file_client.py`
    _write_az_file_client_content(az_file_client_content)
    click.echo(f"{append_functions} functions are successfully added.")


def main():
    cmd(obj={})
