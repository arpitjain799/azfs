from inspect import signature
import re


MODULE_PATTERN = "<module '(?P<module_name>[A-Za-z0-9]+)' from '.+?'>"
CLASS_PATTERN = "<class '(?P<class_name>.*?)'>"


def _decode_types(input_str: str):
    pattern = rf"({MODULE_PATTERN})?({CLASS_PATTERN})?"
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
    result = re.sub(MODULE_PATTERN, r"\g<module_name>", input_str)
    result = re.sub(CLASS_PATTERN, r"\g<class_name>", result)
    return result


def get_signature_and_additional_imports(function: callable) -> (str, str):
    """

    Args:
        function:

    Returns:
        additional_import: additional required import
        ideal_sig: signature

    """
    sig = signature(function)
    # initialize
    additional_import_list = []

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

    return additional_import, ideal_sig
