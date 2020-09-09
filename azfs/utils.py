from typing import Union, Tuple
import re
from azfs.error import (
    AzfsInputError,
    AzfsInvalidPathError
)


class BlobPathDecoder:
    """
    Decode Azure Blob Storage URL format class

    Examples:
        >>> import azfs
        >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
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
    # replace_dict
    _replace_dict = {
        "%A": _STORAGE_ACCOUNT,
        "%T": _STORAGE_TYPE,
        "%C": _CONTAINER,
        "%B": _BLOB
    }
    # pattern
    _decode_path_pattern_list = [
        rf"https://{_STORAGE_ACCOUNT}.{_STORAGE_TYPE}.core.windows.net/{_CONTAINER}?/?{_BLOB}?$",
        rf"/?{_STORAGE_TYPE}/{_STORAGE_ACCOUNT}/{_CONTAINER}/{_BLOB}"
    ]

    def __init__(self, path: Union[None, str] = None):
        self.storage_account_name = None
        # blob: blob or data_lake: dfs
        self.account_type = None
        self.container_name = None
        self.blob_name = None

        # ここでpathが入った場合はすぐに取得
        if path is not None:
            self.storage_account_name, self.account_type, self.container_name, self.blob_name = self._decode(path=path)

    @classmethod
    def add_pattern(cls, pattern: str):
        if pattern.count("%") != 4:
            raise AzfsInputError("`%` should be 4")
        for target, objective in cls._replace_dict.items():
            pattern = pattern.replace(target, objective)
        if pattern.count("%") > 0:
            raise AzfsInputError("`%` should be 0")

        cls._decode_path_pattern_list.append(pattern)
        return pattern

    @staticmethod
    def _decode_path(pattern_path, input_path) -> (str, str, str, str):
        result = re.match(pattern_path, input_path)
        if result:
            return BlobPathDecoder._decode_pattern_block_dict(result.groupdict())
        raise AzfsInputError(f"not matched with {pattern_path}")

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

    @classmethod
    def _decode(cls, path) -> Tuple[str, str, str, str]:
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
        for pattern_path in cls._decode_path_pattern_list:
            try:
                return cls._decode_path(pattern_path, path)
            except AzfsInputError:
                continue
        raise AzfsInvalidPathError(f"Your input path {path} is not matched.")

    def decode(self, path: str):
        self.storage_account_name, self.account_type, self.container_name, self.blob_name = self._decode(path=path)
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
    return _ls_file_and_folder_filter(file_path_list=file_path_list, parent_path=file_path)


def _ls_file_and_folder_filter(file_path_list: list, parent_path: str):
    """

    Args:
        file_path_list: file_name list including parent_path
        parent_path: filter only specified parent_path

    Returns:
        file list

    Examples:
        >>> file_path_list = [
                "file1.csv",
                "file2.csv",
                "file3.csv",
                "dir/file4.csv",
                "dir/file5.csv",
                "dir/some/file6.csv"
            ]
        >>> _ls_file_and_folder_filter(file_path_list, "")
        {'parent_path': None, 'folder': None, 'blob': 'file1.csv'}
        {'parent_path': None, 'folder': None, 'blob': 'file2.csv'}
        {'parent_path': None, 'folder': None, 'blob': 'file3.csv'}
        {'parent_path': None, 'folder': 'dir/', 'blob': 'file4.csv'}
        {'parent_path': None, 'folder': 'dir/', 'blob': 'file5.csv'}
        {'parent_path': None, 'folder': 'dir/', 'blob': 'some/file6.csv'}
        ['dir/', 'file1.csv', 'file2.csv', 'file3.csv']
        >>> _ls_file_and_folder_filter(file_path_list, "dir")
        {'parent_path': None, 'folder': None, 'blob': 'file1.csv'}
        {'parent_path': None, 'folder': None, 'blob': 'file2.csv'}
        {'parent_path': None, 'folder': None, 'blob': 'file3.csv'}
        {'parent_path': 'dir/', 'folder': None, 'blob': 'file4.csv'}
        {'parent_path': 'dir/', 'folder': None, 'blob': 'file5.csv'}
        {'parent_path': 'dir/', 'folder': 'some/', 'blob': 'file6.csv'}
        ['file4.csv', 'file5.csv', 'some/']
        >>> _ls_file_and_folder_filter(file_path_list, "dir/some")
        {'parent_path': None, 'folder': None, 'blob': 'file1.csv'}
        {'parent_path': None, 'folder': None, 'blob': 'file2.csv'}
        {'parent_path': None, 'folder': None, 'blob': 'file3.csv'}
        {'parent_path': None, 'folder': 'dir/', 'blob': 'file4.csv'}
        {'parent_path': None, 'folder': 'dir/', 'blob': 'file5.csv'}
        {'parent_path': 'dir/some/', 'folder': None, 'blob': 'file6.csv'}
        ['file6.csv']
    """
    # check if file_path endswith `/`
    if not parent_path == "":
        parent_path = parent_path if not parent_path.endswith("/") else parent_path[:-1]
    pattern = rf"(?P<parent_path>{parent_path}/)?(?P<folder>.*?/)?(?P<blob>.*)"

    ls_result_list = []
    for fp in file_path_list:
        result = re.match(pattern, fp)
        if result:
            if parent_path == "":
                if result['parent_path'] is None and result['folder'] is None:
                    # append file, if the file is in under the specified parent_path
                    ls_result_list.append(result['blob'])
                elif result['folder'] is not None:
                    # append folder name
                    ls_result_list.append(result['folder'])
            else:
                if result['parent_path'] is not None and result['folder'] is None:
                    # append file, if the file is in under the specified parent_path
                    ls_result_list.append(result['blob'])
                elif result['parent_path'] is not None and result['folder'] is not None:
                    # append folder name
                    ls_result_list.append(result['folder'])
    ls_result_list = list(set(ls_result_list))
    ls_result_list.sort()
    return ls_result_list
