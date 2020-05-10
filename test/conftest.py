import sys
import os
import pytest
import azfs
import pandas as pd

# テスト対象のファイルへのパスを通している
# pytestの設定
PARENT_PATH = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
SOURCE_PATH = PARENT_PATH.rsplit('/', 1)[0]

sys.path.append(f"{SOURCE_PATH}")


@pytest.fixture()
def _download_data(mocker):
    """
    original data is
    data = {"1": {"name": "alice", "age": "10"}, "2": {"name": "bob", "age": "10"}}
    df = pd.DataFrame.from_dict(data, orient="index")
    :param mocker:
    :return:
    """
    return_value = b'name,age\nalice,10\nbob,10\n'
    func_mock = mocker.MagicMock()
    func_mock.return_value = return_value
    yield func_mock


@pytest.fixture()
def _upload_data(mocker):
    """
    :param mocker:
    :return:
    """
    return_value = True
    func_mock = mocker.MagicMock()
    func_mock.return_value = return_value
    yield func_mock


@pytest.fixture()
def _ls(mocker):
    """
    :param mocker:
    :return:
    """
    return_value = ["test1.csv", "test2.csv", "dir/"]
    func_mock = mocker.MagicMock()
    func_mock.return_value = return_value
    yield func_mock


@pytest.fixture()
def _rm(mocker):
    """
    :param mocker:
    :return:
    """
    return_value = True
    func_mock = mocker.MagicMock()
    func_mock.return_value = return_value
    yield func_mock


@pytest.fixture()
def var_df() -> pd.DataFrame:
    data = {"1": {"name": "alice", "age": "10"}, "2": {"name": "bob", "age": "10"}}
    df = pd.DataFrame.from_dict(data, orient="index")
    yield df


@pytest.fixture()
def var_azc() -> azfs.AzFileClient:
    azc = azfs.AzFileClient()
    yield azc
