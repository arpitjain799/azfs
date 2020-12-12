from inspect import signature
import os

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


@cmd.command("decorator")
@click.pass_context
def decorator(ctx):
    """
    Display the function to deploy based script `app.py`.

    """
    cli_factory: CliFactory = ctx.obj['factory']
    _export_decorator: azfs.az_file_client.ExportDecorator = cli_factory.load_export_decorator()
    click.echo(_export_decorator.functions)

    az_file_client_path = f"{azfs.__file__.rsplit('/', 1)[0]}/az_file_client.py"

    with open(az_file_client_path, "r") as f:
        az_file_client_content = f.readlines()

    main_file_index = len(az_file_client_content)
    for main_file_index, content in enumerate(az_file_client_content):
        if "# end of the main file" in content:
            break

    az_file_client_content = az_file_client_content[:main_file_index+1]
    for f in _export_decorator.functions:
        sig = signature(f['function'])
        ideal_sig = str(sig)
        if "()" in ideal_sig:
            ideal_sig = ideal_sig.replace(")", "**kwargs)")
        else:
            ideal_sig = ideal_sig.replace(")", ", **kwargs)")

        ideal_sig = ideal_sig.replace("pandas.core.frame.DataFrame", "pd.DataFrame")
        new_mock_function: str = MOCK_FUNCTION % (f['register_as'], ideal_sig)

        new_mock_function_content = [f"{s}\n" for s in new_mock_function.split("\n")]
        az_file_client_content.extend(new_mock_function_content)

    with open(az_file_client_path, "w") as f:
        f.writelines(az_file_client_content)


def main():
    cmd(obj={})
