

class ExportDecorator:
    def __init__(self):
        self.function_export_target_keyword_dict = {}

    def register(self):
        def _wrapper(func: callable):
            self.function_export_target_keyword_dict.update(
                {func.__name__: func}
            )
            return func
        return _wrapper

    __call__ = register
