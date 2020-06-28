from typing import Union
import re
from azfs.error import (
    AzfsInputError
)


class BlobPathDecoder:
    """
    Decode Azure Blob Storage URL format class

    Examples:
        >>> import azfs
        >>> path = "https://testazfs.blob.core.windows.net/test_caontainer/test1.csv"
        >>> blob_path_decoder = azfs.BlobPathDecoder()
        >>> blob_path_decoder.decode(path=path).get()
        (testazfs, blob, test_container, test1.csv)
        >>> blob_path_decoder.decode(path=path).get_with_url()
        (https://testazfs.blob.core.windows.net", blob, test_container, test1.csv)

    """
    # pattern blocks
    _STORAGE_ACCOUNT = "(?P<storage_account>[a-z0-9]*)"
    _STORAGE_TYPE = "(?P<storage_type>dfs|blob|queue)"
    _CONTAINER = "(?P<container>[^/.]*)"
    _BLOB = "(?P<blob>.+)"
    # pattern
    _BLOB_URL_PATTERN = rf"https://{_STORAGE_ACCOUNT}.{_STORAGE_TYPE}.core.windows.net/{_CONTAINER}?/?{_BLOB}?$"
    _SIMPLE_PATTERN = rf"/?{_STORAGE_TYPE}/{_STORAGE_ACCOUNT}/{_CONTAINER}/{_BLOB}"

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
    def _decode_path_blob_name(path: str) -> (str, str, str, str):
        """
        ex: https://test.dfs.core.windows.net/test/dir/test_file.csv
        Args:
            path: decode target path

        Returns:
            tuple of str

        Raises:
            AzfsInputError: when pattern not matched
        """
        result = re.match(BlobPathDecoder._BLOB_URL_PATTERN, path)
        if result:
            return BlobPathDecoder._decode_pattern_block_dict(result.groupdict())
        raise AzfsInputError(f"not matched with {BlobPathDecoder._BLOB_URL_PATTERN}")

    @staticmethod
    def _decode_path_without_url(path: str) -> (str, str, str, str):
        """
        ex: /dfs/test/test/test_file.csv
        Args:
            path: decode target path

        Returns:
            tuple of str

        Raises:
            AzfsInputError: when pattern not matched
        """
        result = re.match(BlobPathDecoder._SIMPLE_PATTERN, path)
        if result:
            return BlobPathDecoder._decode_pattern_block_dict(result.groupdict())
        raise AzfsInputError(f"not matched with {BlobPathDecoder._SIMPLE_PATTERN}")

    @staticmethod
    def _decode_pattern_block_dict(pattern_block_dict: dict) -> (str, str, str, str):
        """
        Args:
            pattern_block_dict:

        Returns:
            tuple of str

        Examples:
            block_dict = {
                'storage_account': 'test',
                'storage_type': 'blob',
                'container': '',
                'blob': None
            }

            BlobPathDecoder._decode_pattern_block_dict(pattern_block_dict=block_dict)
            (test, blob, "", "")

        """
        # if finding regex-pattern with ?P<name>, `None` appears in value
        # so replace None to ""
        result_dict = {k: (v if v is not None else "") for k, v in pattern_block_dict.items()}
        # get specified key
        storage_account_name = result_dict["storage_account"]
        account_type = result_dict["storage_type"]
        container_name = result_dict["container"]
        blob_name = result_dict["blob"]
        return storage_account_name, account_type, container_name, blob_name

    @staticmethod
    def _decode_path(path: str) -> (str, str, str, str):
        """
        decode input [path] such as
        * https://([a-z0-9]*).(dfs|blob|queue).core.windows.net/(.*?)/(.*),
        * ([a-z0-9]*)/(.+?)/(.*)

        dfs: data_lake, blob: blob
        Args:
            path:

        Returns:
            tuple of str

        Raises:
            AzfsInputError: when pattern not matched
        """
        function_list = [
            BlobPathDecoder._decode_path_blob_name,
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
    特定のフォルダ以下にあるファイル名を取得する。
    :param file_path_list:
    :param file_path:
    :return:
    """
    filtered_file_path_list = []
    if not file_path == "":
        # check if file_path endswith `/`
        file_path = file_path if not file_path.endswith("/") else file_path[:-1]
        file_path_pattern = rf"(?P<file_path>{file_path}/)(?P<blob>.*)"
        for fp in file_path_list:
            result = re.match(file_path_pattern, fp)
            if result:
                filtered_file_path_list.append(result.group("blob"))
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
    file_path_block = "?P<file_path>"
    if not file_path == "":
        # check if file_path endswith `/`
        file_path = file_path if not file_path.endswith("/") else file_path[:-1]
        file_path_block = f"?P<file_path>{file_path}/"
    # create match pattern
    file_path_pattern = rf"({file_path_block})(?P<folder>.*?/)(?P<rest>.*)"

    folders_in_file_path = []
    for fp in file_path_list:
        result = re.match(file_path_pattern, fp)
        if result:
            folders_in_file_path.append(result.group("folder"))

    return list(set(folders_in_file_path))
