class AzfsBaseError(Exception):
    pass


class AzfsInputError(AzfsBaseError):
    pass


class AzfsInvalidPathError(AzfsBaseError):
    pass
