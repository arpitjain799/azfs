import pandas as pd
import io
import re
import json
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import (
    DataLakeFileClient
)
from azure.storage.blob import BlobClient
from typing import Union
from azfs.error import (
    AzfsInputError
)


def _decode_path(path: str) -> (str, str, str):
    """
    :param path:
    :return:
    """
    storage_account_name = None
    # dfs: data_lake, blob: blob
    account_kind = "dfs"
    container_name = None
    key = None

    # pattern
    url_pattern = r"https://([a-z0-9]*).(dfs|blob).core.windows.net/(.*?)/(.*)"
    storage_pattern = r"([a-z0-9]*)/(.+?)/(.*)"

    url_result = re.match(url_pattern, path)
    storage_result = re.match(storage_pattern, path)
    if url_result:
        storage_account_name = url_result.group(1)
        account_kind = url_result.group(2)
        container_name = url_result.group(3)
        key = url_result.group(4)

    if storage_result:
        storage_account_name = storage_result.group(1)
        container_name = storage_result.group(2)
        key = storage_result.group(3)
    if storage_account_name is None:
        raise AzfsInputError(f"入力されたpath[{path}]が不正です")
    return f"https://{storage_account_name}.{account_kind}.core.windows.net", account_kind, container_name, key


def _get_file_client(
        storage_account_url,
        account_kind,
        file_system,
        file_path,
        msi: DefaultAzureCredential) -> Union[DataLakeFileClient, BlobClient]:
    """

    :param storage_account_url:
    :param account_kind:
    :param file_system:
    :param file_path:
    :param msi:
    :return:
    """
    if account_kind == "dfs":
        file_client = DataLakeFileClient(
            storage_account_url,
            file_system,
            file_path,
            credential=msi)
        return file_client
    elif account_kind == "blob":
        file_client = BlobClient(
            account_url=storage_account_url,
            container_name=file_system,
            blob_name=file_path,
            credential=msi
        )
        return file_client
    else:
        raise ValueError("account_kindが不正です")


def _download_data(path: str, msi: DefaultAzureCredential) -> Union[bytes, str]:
    """
    storage accountのタイプによってfile_clientを変更し、
    特定のファイルを取得する関数
    :param path:
    :param msi:
    :return:
    """
    storage_account_url, account_kind, file_system, file_path = _decode_path(path)
    if account_kind == "dfs":
        file_client = _get_file_client(
            storage_account_url=storage_account_url,
            account_kind=account_kind,
            file_system=file_system,
            file_path=file_path,
            msi=msi)
        file_bytes = file_client.download_file().readall()
        return file_bytes
    elif account_kind == "blob":
        file_client = _get_file_client(
            storage_account_url=storage_account_url,
            account_kind=account_kind,
            file_system=file_system,
            file_path=file_path,
            msi=msi)
        file_bytes = file_client.download_blob().readall()
        return file_bytes


def read_csv(path: str, msi: DefaultAzureCredential) -> pd.DataFrame:
    """

    :param path:
    :param msi:
    :return:
    """
    file_bytes = _download_data(path, msi)
    if type(file_bytes) is bytes:
        file_to_read = io.BytesIO(file_bytes)
    else:
        file_to_read = file_bytes
    return pd.read_csv(file_to_read)


def write_csv(path: str, df: pd.DataFrame, msi) -> bool:
    """
    output pandas dataframe to csv file in Datalake storage.
    Note: Unavailable for large loop processing!
    https://<storage_account>.dfs.core.windows.net/<file_system>/<file_name>
    or
    <storage_account>/<file_system>/<file_name>
    """
    storage_account_url, account_kind, file_system, file_path = _decode_path(path)
    file_client = _get_file_client(
            storage_account_url=storage_account_url,
            account_kind=account_kind,
            file_system=file_system,
            file_path=file_path,
            msi=msi)
    csv_str = df.to_csv(encoding="utf-8")
    _ = file_client.create_file()
    _ = file_client.append_data(csv_str, offset=0)
    _ = file_client.flush_data(len(csv_str))
    return True


def read_json(path: str, msi) -> dict:
    """
    read json file in Datalake storage.
    Note: Unavailable for large loop processing!
    https://<storage_account>.dfs.core.windows.net/<file_system>/<file_name>
    or
    <storage_account>/<file_system>/<file_name>
    """
    file_bytes = _download_data(path, msi)
    return json.loads(file_bytes)


def write_json(path: str, dict_data: dict, msi) -> bool:
    """
    output dict to json file in Datalake storage.
    Note: Unavailable for large loop processing!
    https://<storage_account>.dfs.core.windows.net/<file_system>/<file_name>
    or
    <storage_account>/<file_system>/<file_name>
    """
    storage_account_url, account_kind, file_system, file_path = _decode_path(path)
    file_client = _get_file_client(
            storage_account_url=storage_account_url,
            account_kind=account_kind,
            file_system=file_system,
            file_path=file_path,
            msi=msi)
    data = json.dumps(dict_data)
    _ = file_client.create_file()
    _ = file_client.append_data(data, offset=0)
    _ = file_client.flush_data(len(data))
    return True
