from typing import Optional


def _generate_parameter_args(
        additional_args: Optional[str] = None,
        storage_account: Optional[str] = None,
        storage_type: Optional[str] = None,
        container: Optional[str] = None,
        key: Optional[str] = None,
        output_parent_path: Optional[str] = None,
        file_name_prefix: Optional[str] = None,
        file_name: Optional[str] = None,
        file_name_suffix: Optional[str] = None,
        export: Optional[str] = None,
        format_type: Optional[str] = None
        ) -> str:
    """

    Args:
        additional_args:
        storage_account:
        storage_type:
        container:
        key:
        output_parent_path:
        file_name_prefix:
        file_name:
        file_name_suffix:
        export:
        format_type:

    Returns:
        argument example for the function

    """
    indent_ = "\n        "
    basic_args_ = "_{kwrd}: ({_type}) {exp}, default:={default}"
    args_dict = [
        {
            "kwrd": "storage_account",
            "_type": "str",
            "exp": "storage account",
            "default": storage_account
        },
        {
            "kwrd": "storage_type",
            "_type": "str",
            "exp": "`blob` or `dfs`",
            "default": storage_type
        },
        {
            "kwrd": "container",
            "_type": "str",
            "exp": "container",
            "default": container
        },
        {
            "kwrd": "key",
            "_type": "str",
            "exp": "as same as folder name",
            "default": key
        },
        {
            "kwrd": "output_parent_path",
            "_type": "str",
            "exp": "ex. https://st.blob.core.windows.net/container/{_key}/{file_name}",
            "default": output_parent_path
        },
        {
            "kwrd": "file_name_prefix",
            "_type": "str",
            "exp": "{file_name_prefix}{file_name}",
            "default": file_name_prefix
        },
        {
            "kwrd": "file_name",
            "_type": "str, List[str]",
            "exp": "file_name",
            "default": file_name
        },
        {
            "kwrd": "file_name_suffix",
            "_type": "str",
            "exp": "{file_name}{file_name_suffix}",
            "default": file_name_suffix
        },
        {
            "kwrd": "export",
            "_type": "bool",
            "exp": "export if True",
            "default": export
        },
        {
            "kwrd": "format_type",
            "_type": "str",
            "exp": "`csv` or `pickle`",
            "default": format_type
        },
    ]

    if additional_args is not None:
        args_list = [
            f"\n        == params for {additional_args} ==",
        ]
        args_list.extend(
            [f"{indent_}_{additional_args}{basic_args_.format(**d)}" for d in args_dict]
        )
        return "".join(args_list)
    else:
        args_list = [
            f"\n        == params for default ==",
        ]
        args_list.extend(
            [f"{indent_}{basic_args_.format(**d)}" for d in args_dict]
        )
        return "".join(args_list)


def _append_docs(docstring: Optional[str], additional_args_list: list, **kwargs) -> str:
    """
    append/generate docstring

    Args:
        docstring: already written docstring
        additional_args_list:
        **kwargs:

    Returns:
        `docstring`
    """
    result_list = []
    if docstring is not None:
        for s in docstring.split("\n\n"):
            if "Args:" in s:
                # set `None` to describe `default` parameter
                additional_args_list_ = [None]
                # set `{keyword_list}` parameters
                additional_args_list_.extend(additional_args_list)
                args_list = [_generate_parameter_args(arg, **kwargs) for arg in additional_args_list_]
                addition_s = f"{s}{''.join(args_list)}"
                result_list.append(addition_s)
            else:
                result_list.append(s)
        return "\n\n".join(result_list)
    else:
        result_list.append("Args:")
        # set `None` to describe `default` parameter
        additional_args_list_ = [None]
        # set `{keyword_list}` parameters
        additional_args_list_.extend(additional_args_list)
        args_list = [_generate_parameter_args(arg, **kwargs) for arg in additional_args_list_]
        addition_s = ''.join(args_list)
        result_list.append(addition_s)
        return "\n\n".join(result_list)


def append_docs(docstring: Optional[str], additional_args_list: list, **kwargs) -> str:
    """

    Args:
        docstring:
        additional_args_list:
        **kwargs:

    Returns:
        re-generated-`docstring`

    Examples:
        >>> def some_func(a: str) -> pd.DataFrame:
        ...     '''
        ...     the functions is ...
        ...
        ...     Args:
        ...        a: hello
        ...     '''
        ...     return pd.DataFrame()
        >>>
        >>> print(append_docs(some_func.__doc__, ["prod"]))
        ... # generated docstrings ...
    """
    return _append_docs(docstring=docstring, additional_args_list=additional_args_list, **kwargs)
