from typing import Optional


class ExportDecorator:
    def __init__(self):
        self.functions = []

    def register(self, _as: Optional[str] = None):
        def _wrapper(func: callable):
            func_name = func.__name__
            self.functions.append(
                {
                    "function_name": func_name,
                    "register_as": _as if _as is not None else func_name,
                    "function": func
                }
            )
            return func
        return _wrapper

    __call__ = register
