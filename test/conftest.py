import bz2
import gzip
import io
import json
import lzma
import os
import pickle
import sys
import pandas as pd
import pytest
import azfs

# テスト対象のファイルへのパスを通している
# pytestの設定
PARENT_PATH = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
SOURCE_PATH = PARENT_PATH.rsplit('/', 1)[0]

sys.path.append(f"{SOURCE_PATH}")


@pytest.fixture()
def _get_csv(mocker):
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
def _get_csv_gz(mocker):
    """
    original data is
    data = {"1": {"name": "alice", "age": "10"}, "2": {"name": "bob", "age": "10"}}
    df = pd.DataFrame.from_dict(data, orient="index")

    Args:
        mocker:

    Returns:

    """
    data = b'name,age\nalice,10\nbob,10\n'
    return_value = gzip.compress(data)
    func_mock = mocker.MagicMock()
    func_mock.return_value = return_value
    yield func_mock


@pytest.fixture()
def _get_table(mocker):
    """
    original data is
    data = {"1": {"name": "alice", "age": "10"}, "2": {"name": "bob", "age": "10"}}
    df = pd.DataFrame.from_dict(data, orient="index")
    :param mocker:
    :return:
    """
    return_value = b'\tname\tage\n1\talice\t10\n2\tbob\t10\n'
    func_mock = mocker.MagicMock()
    func_mock.return_value = return_value
    yield func_mock


@pytest.fixture()
def _get_pickle(mocker):
    data = {"1": {"name": "alice", "age": "10"}, "2": {"name": "bob", "age": "10"}}
    df = pd.DataFrame.from_dict(data, orient="index")
    p = pickle.dumps(df)
    return_value = io.BytesIO(p)
    func_mock = mocker.MagicMock()
    func_mock.return_value = return_value
    yield func_mock


@pytest.fixture()
def _get_pickle_gzip(mocker):
    data = {"1": {"name": "alice", "age": "10"}, "2": {"name": "bob", "age": "10"}}
    df = pd.DataFrame.from_dict(data, orient="index")
    p = pickle.dumps(df)
    return_value = io.BytesIO(gzip.compress(p))
    func_mock = mocker.MagicMock()
    func_mock.return_value = return_value
    yield func_mock


@pytest.fixture()
def _get_pickle_bz2(mocker):
    data = {"1": {"name": "alice", "age": "10"}, "2": {"name": "bob", "age": "10"}}
    df = pd.DataFrame.from_dict(data, orient="index")
    p = pickle.dumps(df)
    return_value = io.BytesIO(bz2.compress(p))
    func_mock = mocker.MagicMock()
    func_mock.return_value = return_value
    yield func_mock


@pytest.fixture()
def _get_pickle_xz(mocker):
    data = {"1": {"name": "alice", "age": "10"}, "2": {"name": "bob", "age": "10"}}
    df = pd.DataFrame.from_dict(data, orient="index")
    p = pickle.dumps(df)
    return_value = io.BytesIO(lzma.compress(p))
    func_mock = mocker.MagicMock()
    func_mock.return_value = return_value
    yield func_mock


@pytest.fixture()
def _get_json(mocker):
    """
    :param mocker:
    :return:
    """
    return_value = {"1": {"name": "alice", "age": "10"}, "2": {"name": "bob", "age": "10"}}
    func_mock = mocker.MagicMock()
    func_mock.return_value = json.dumps(return_value)
    yield func_mock


@pytest.fixture()
def _put(mocker):
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
def _ls_for_glob(mocker):
    """
    :param mocker:
    :return:
    """
    return_value = [
        "root_folder/test1.csv",
        "root_folder/test2.csv",
        "root_folder/test1.json",
        "root_folder/dir1/test1.csv",
        "root_folder/dir1/test2.csv",
        "root_folder/dir1/test1.json",
        "root_folder/dir2/test1.csv",
        "root_folder/dir2/test2.csv",
        "root_folder/dir2/test1.json",
    ]
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
def var_json() -> pd.DataFrame:
    data = {"1": {"name": "alice", "age": "10"}, "2": {"name": "bob", "age": "10"}}
    yield data


@pytest.fixture()
def var_df() -> pd.DataFrame:
    data = {"1": {"name": "alice", "age": "10"}, "2": {"name": "bob", "age": "10"}}
    df = pd.DataFrame.from_dict(data, orient="index")
    yield df


@pytest.fixture()
def var_azc() -> azfs.AzFileClient:
    azc = azfs.AzFileClient()
    yield azc
