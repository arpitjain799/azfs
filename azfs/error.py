from typing import Optional


class AzfsBaseError(Exception):
    pass


class AzfsInputError(AzfsBaseError):
    pass


class AzfsInvalidPathError(AzfsBaseError):
    pass


class AzfsImportDecoratorError(AzfsBaseError):
    MESSAGE = ""

    def __init__(self, message: Optional[str] = None):
        self.message = message if message is not None else self.MESSAGE

    def __str__(self):
        return self.message


class AzfsDecoratorFileFormatError(AzfsImportDecoratorError):
    MESSAGE = "file format must be `csv` or `pickle`"


class AzfsDecoratorSizeNotMatchedError(AzfsImportDecoratorError):
    MESSAGE = "size of output path and function response not matched"


class AzfsDecoratorReturnTypeError(AzfsImportDecoratorError):
    MESSAGE = "return type of the given function must be `pd.DataFrame`"
