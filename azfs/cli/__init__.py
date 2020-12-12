import os

import click
from .factory import CliFactory
from .constants import WELCOME_PROMPT


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


def main():
    cmd(obj={})
