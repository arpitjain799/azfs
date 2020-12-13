import pandas as pd
from typing import Tuple
from azfs import export_decorator


@export_decorator.register()
def example_1(name: str, b: int, c: str) -> pd.DataFrame:
    return pd.DataFrame()


@export_decorator.register()
def example_2() -> Tuple[pd.DataFrame, pd.DataFrame]:
    return pd.DataFrame(), pd.DataFrame()
