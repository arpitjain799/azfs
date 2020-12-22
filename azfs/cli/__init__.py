from inspect import signature
import os
import re
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

    def _decode_types(input_str: str):
        pattern = r"(<module '(?P<module_name>[A-Za-z0-9]+)' from '.+?'>)?(<class '(?P<class_name>.*?)'>)?"
        result = re.match(pattern, input_str)
        if not result:
            return "", None
        else:
            result_dict = result.groupdict()
            return _get_module_and_imports(**result_dict)

    def _get_module_and_imports(module_name: str, class_name: str) -> (str, str):
        if module_name is not None:
            return module_name, module_name
        elif class_name is not None:
            if "." in class_name:
                import_str = class_name.split(".", 1)
                return class_name, import_str[0]
            else:
                return class_name, ""
        else:
            raise ValueError

    def _decode_signature(input_str: str):
        module_pattern = r"<module '(?P<module_name>[A-Za-z0-9]+)' from '.+?'>"
        class_pattern = r"<class '(?P<class_name>.*?)'>"

        result = re.sub(module_pattern, r"\g<module_name>", input_str)
        result = re.sub(class_pattern, r"\g<class_name>", result)
        return result

    new_lines = []
    append_functions = len(export_decorator.functions)

    for f in export_decorator.functions:
        # initialize
        additional_import_list = []

        function_name = f['register_as']
        sig = signature(f['function'])

        # argument parameters
        for signature_params in sig.parameters:
            annotation_candidate = sig.parameters[signature_params].annotation
            _, import_candidate = _decode_types(str(annotation_candidate))
            additional_import_list.append(import_candidate)

        # return parameters
        return_annotation = sig.return_annotation
        return_annotation_type = type(return_annotation)
        if str(return_annotation) == "<class 'inspect._empty'>":
            # inspect._empty is not accessible
            pass
        elif str(return_annotation_type) == "<class 'typing._GenericAlias'>":
            # typing._GenericAlias is also not accessible
            pass
        elif return_annotation_type == tuple:
            for signature_params in return_annotation:
                _, import_candidate = _decode_types(str(signature_params))
                additional_import_list.append(import_candidate)
        else:
            _, import_candidate = _decode_types(str(return_annotation))
            additional_import_list.append(import_candidate)

        ideal_sig = _decode_signature(str(sig))
        if "()" in ideal_sig:
            ideal_sig = ideal_sig.replace(")", "**kwargs)", 1)
        else:
            ideal_sig = ideal_sig.replace(")", ", **kwargs)", 1)

        # create additional import
        additional_import_candidate = [s for s in set(additional_import_list) if len(s) > 0]
        additional_import = ""
        if len(additional_import_candidate) > 0:
            additional_import = f"import {', '.join(additional_import_candidate)}\n"

        # create mock function
        new_mock_function: str = MOCK_FUNCTION % (additional_import, function_name, ideal_sig)

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
