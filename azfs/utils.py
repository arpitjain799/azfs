from typing import Union
import re
from azfs.error import (
    AzfsInputError
)


class BlobPathDecoder:
    def __init__(self, path: Union[None, str] = None):
        self.storage_account_name = None
        # blob: blob or data_lake: dfs
        self.account_type = None
        self.container_name = None
        self.blob_name = None

        # ここでpathが入った場合はすぐに取得
        if path is not None:
            self.storage_account_name, self.account_type, self.container_name, self.blob_name = self._decode_path(
                path=path)

    @staticmethod
    def _decode_path_storage_account_name(path: str) -> (str, str, str, str):
        url_pattern = r"https://([a-z0-9]*).(dfs|blob).core.windows.net/(.*)"
        result = re.match(url_pattern, path)
        if result:
            storage_account_name = result.group(1)
            account_type = result.group(2)
            container_name = result.group(3)
            return storage_account_name, account_type, container_name, ""
        raise AzfsInputError(f"not matched with {url_pattern}")

    @staticmethod
    def _decode_path_container_name(path: str) -> (str, str, str, str):
        url_pattern = r"https://([a-z0-9]*).(dfs|blob).core.windows.net/(.*?)/"
        result = re.match(url_pattern, path)
        if result:
            storage_account_name = result.group(1)
            account_type = result.group(2)
            container_name = result.group(3)
            return storage_account_name, account_type, container_name, ""
        raise AzfsInputError(f"not matched with {url_pattern}")

    @staticmethod
    def _decode_path_blob_name(path: str) -> (str, str, str, str):
        url_pattern = r"https://([a-z0-9]*).(dfs|blob).core.windows.net/(.*?)/(.+)"
        result = re.match(url_pattern, path)
        if result:
            storage_account_name = result.group(1)
            account_type = result.group(2)
            container_name = result.group(3)
            blob_name = result.group(4)
            return storage_account_name, account_type, container_name, blob_name
        raise AzfsInputError(f"not matched with {url_pattern}")

    @staticmethod
    def _decode_path_without_url(path: str) -> (str, str, str, str):
        url_pattern = r"(dfs|blob)/([a-z0-9]*)/(.+?)/(.*)"
        result = re.match(url_pattern, path)
        if result:
            storage_account_name = result.group(2)
            account_type = result.group(1)
            container_name = result.group(3)
            blob_name = result.group(4)
            return storage_account_name, account_type, container_name, blob_name
        raise AzfsInputError(f"not matched with {url_pattern}")

    @staticmethod
    def _decode_path(path: str) -> (str, str, str, str):
        """
        decode input [path] such as
        * https://([a-z0-9]*).(dfs|blob).core.windows.net/(.*?)/(.*),
        * ([a-z0-9]*)/(.+?)/(.*)

        dfs: data_lake, blob: blob
        :param path:
        :return:
        """
        function_list = [
            BlobPathDecoder._decode_path_blob_name,
            BlobPathDecoder._decode_path_container_name,
            BlobPathDecoder._decode_path_storage_account_name,
            BlobPathDecoder._decode_path_without_url
        ]
        for func in function_list:
            try:
                storage_account_name, account_type, container_name, blob_name = func(path=path)
            except AzfsInputError:
                continue
            else:
                return storage_account_name, account_type, container_name, blob_name
        raise AzfsInputError("合致するパターンがありません")

    def decode(self, path: str):
        self.storage_account_name, self.account_type, self.container_name, self.blob_name = self._decode_path(path=path)
        return self

    def get(self) -> (str, str, str, str):
        return \
            self.storage_account_name, \
            self.account_type, \
            self.container_name, \
            self.blob_name

    def get_with_url(self) -> (str, str, str, str):
        return \
            f"https://{self.storage_account_name}.{self.account_type}.core.windows.net", \
            self.account_type, \
            self.container_name, \
            self.blob_name

# ================ #
# filter based `/` #
# ================ #


def ls_filter(file_path_list: list, file_path: str):
    filtered_list = []
    filtered_list.extend(_ls_get_file_name(file_path_list=file_path_list, file_path=file_path))
    filtered_list.extend(_ls_get_folder_name(file_path_list=file_path_list, file_path=file_path))
    return filtered_list


def _ls_get_file_name(file_path_list: list, file_path: str):
    """
    特定のフォルダ以下にあるファイル名を取得する
    :param file_path_list:
    :param file_path:
    :return:
    """
    filtered_file_path_list = []
    if not file_path == "":
        file_path_pattern = rf"({file_path}/)(.*)"
        for fp in file_path_list:
            result = re.match(file_path_pattern, fp)
            if result:
                filtered_file_path_list.append(result.group(2))
    else:
        filtered_file_path_list = file_path_list
    return [f for f in filtered_file_path_list if "/" not in f]


def _ls_get_folder_name(file_path_list: list, file_path: str):
    """
    特定のフォルダ以下にあるフォルダ名を取得する
    :param file_path_list:
    :param file_path:
    :return:
    """
    folders_in_file_path = []
    if not file_path == "":
        file_path_pattern = rf"({file_path}/)(.*?/)(.*)"
        for fp in file_path_list:
            result = re.match(file_path_pattern, fp)
            if result:
                folders_in_file_path.append(result.group(2))
    else:
        file_path_pattern = rf"(.*?/)(.*)"
        for fp in file_path_list:
            result = re.match(file_path_pattern, fp)
            if result:
                folders_in_file_path.append(result.group(1))
    return list(set(folders_in_file_path))
