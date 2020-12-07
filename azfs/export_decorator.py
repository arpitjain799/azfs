

class ExportDecorator:
    def __init__(self):
        self.functions = []

    def register(self):
        def _wrapper(func: callable):
            self.functions.append(
                {"function_name": func.__name__, "function": func}
            )
            return func
        return _wrapper

    __call__ = register
