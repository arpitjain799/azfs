import platform
import pytest
from typing import Optional
from azfs.function_manipulation import get_signature_and_additional_imports
import pandas as pd

version: tuple = platform.python_version_tuple()
major = int(version[0])
minor = int(version[1])


def test_signature_manipulation():
    def some_func_1(a: str) -> pd.DataFrame:
        return pd.DataFrame()

    response = get_signature_and_additional_imports(some_func_1)
    assert response == ('import pandas\n', '(a: str, **kwargs) -> pandas.core.frame.DataFrame')

    def some_func_2() -> pd.DataFrame:
        return pd.DataFrame()

    response = get_signature_and_additional_imports(some_func_2)
    assert response == ('import pandas\n', '(**kwargs) -> pandas.core.frame.DataFrame')

    def some_func_3() -> (pd.DataFrame, pd.DataFrame):
        return pd.DataFrame(), pd.DataFrame()

    response = get_signature_and_additional_imports(some_func_3)
    assert response == ('import pandas\n', '(**kwargs) -> (pandas.core.frame.DataFrame, pandas.core.frame.DataFrame)')

    def some_func_4():
        return pd.DataFrame(), pd.DataFrame()

    response = get_signature_and_additional_imports(some_func_4)
    assert response == ('', '(**kwargs)')

    def some_func_5() -> Optional[str]:
        return ""

    # difference after python 3.9.0
    if minor <= 8:
        response = get_signature_and_additional_imports(some_func_5)
        assert response == ('', '(**kwargs) -> Union[str, NoneType]')
    else:
        with pytest.raises(ValueError):
            _ = get_signature_and_additional_imports(some_func_5)
