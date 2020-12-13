class AzfsBaseError(Exception):
    pass


class AzfsInputError(AzfsBaseError):
    pass


class AzfsInvalidPathError(AzfsBaseError):
    pass


class AzfsImportDecoratorError(AzfsBaseError):
    pass


class AzfsDecoratorFileFormatError(AzfsImportDecoratorError):
    pass


