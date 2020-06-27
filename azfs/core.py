import io
import re
from typing import Union, Optional
import json
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError
from azfs.clients import AzfsClient
from azfs.error import AzfsInputError
from azfs.utils import (
    BlobPathDecoder,
    ls_filter
)


class AzFileClient:
    """

    azfs can

    * list files in blob (also with wildcard *),
    * check if file exists,
    * read csv as pd.DataFrame, and json as dict from blob,
    * write pd.DataFrame as csv, and dict as json to blob,

    Examples:
        >>> import azfs
        >>> import pandas as pd
        >>> credential = "[your credential]"
        >>> azc = azfs.AzFileClient()
        >>> path = "your blob file url, starts with https://..."
        you can read and write csv file in azure blob storage
        >>> df = azc.read_csv(path=path)
        >>> azc.write_csv(path=path, df=df)
        Using `with` statement, you can use `pandas`-like methods
        >>> with azc:
        >>>     df = pd.read_csv_az(path)
        >>>     df.to_csv_az(path)

    """

    def __init__(
            self,
            credential: Optional[Union[str, DefaultAzureCredential]] = None):
        """

        :param credential: if string, Blob Storage -> Access Keys -> Key
        """
        if credential is None:
            credential = DefaultAzureCredential()
        self.credential = credential

    def __enter__(self):
        """
        with句でのread_csv_azとto_csv_azの関数追加処理
        :return:
        """
        pd.__dict__['read_csv_az'] = self.read_csv
        pd.DataFrame.to_csv_az = self.to_csv(self)
        return self

    def __exit__(self, exec_type, exec_value, traceback):
        """
        with句で追加したread_csv_azとto_csv_azの削除
        :param exec_type:
        :param exec_value:
        :param traceback:
        :return:
        """
        pd.__dict__.pop('read_csv_az')
        pd.DataFrame.to_csv_az = None

    @staticmethod
    def to_csv(az_file_client):
        def inner(self, path, **kwargs):
            df = self if isinstance(self, pd.DataFrame) else None
            return az_file_client.write_csv(path=path, df=df, **kwargs)
        return inner

    def exists(self, path: str) -> bool:
        """
        check if specified file exists or not.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_caontainer/test1.csv``

        Returns:
            ``True`` if files exists, otherwise ``False``

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_caontainer/test1.csv"
            >>> azc.exists(path=path)
            True
            >>> path = "https://testazfs.blob.core.windows.net/test_caontainer/not_exist_test1.csv"
            >>> azc.exists(path=path)
            False

        """
        try:
            _ = self._get(path=path)
        except ResourceNotFoundError:
            return False
        else:
            return True

    def ls(self, path: str, attach_prefix: bool = False):
        """
        list blob file from blob or dfs.

        Args:
            path: Azure Blob path URL format, ex: https://testazfs.blob.core.windows.net/test_caontainer
            attach_prefix: return full_path if True, return only name

        Returns:
            list of azure blob files

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_caontainer"
            >>> azc.ls(path)
            [
                "test1.csv",
                "test2.csv",
                "test3.csv",
                "directory_1",
                "directory_2"
            ]
            >>> azc.ls(path=path, attach_prefix=True)
            [
                "https://testazfs.blob.core.windows.net/test_caontainer/test1.csv",
                "https://testazfs.blob.core.windows.net/test_caontainer/test2.csv",
                "https://testazfs.blob.core.windows.net/test_caontainer/test3.csv",
                "https://testazfs.blob.core.windows.net/test_caontainer/directory_1",
                "https://testazfs.blob.core.windows.net/test_caontainer/directory_2"
            ]

        """
        _, account_kind, _, file_path = BlobPathDecoder(path).get_with_url()
        file_list = AzfsClient.get(account_kind, credential=self.credential).ls(path=path, file_path=file_path)
        if account_kind in ["dfs", "blob"]:
            file_name_list = ls_filter(file_path_list=file_list, file_path=file_path)
            if attach_prefix:
                path = path if path.endswith("/") else f"{path}/"
                file_full_path_list = [f"{path}{f}" for f in file_name_list]
                return file_full_path_list
            else:
                return file_name_list
        elif account_kind in ["queue"]:
            return file_list

    def walk(self, path: str, max_depth=2):
        pass

    def cp(self, src_path: str, dst_path: str, overwrite=False):
        """
        copy the data from `src_path` to `dst_path`

        Args:
            src_path:
            dst_path:
            overwrite:

        Returns:

        """
        if src_path == dst_path:
            raise AzfsInputError("src_path and dst_path must be different")
        if (not overwrite) and self.exists(dst_path):
            raise AzfsInputError(f"{dst_path} is already exists. Please set `overwrite=True`.")
        data = self._get(path=src_path)
        if type(data) is io.BytesIO:
            self._put(path=dst_path, data=data.read())
        elif type(data) is bytes:
            self._put(path=dst_path, data=data)
        return True

    def rm(self, path: str) -> bool:
        """
        delete the file in blob
        :param path:
        :return:
        """
        _, account_kind, _, _ = BlobPathDecoder(path).get_with_url()
        return AzfsClient.get(account_kind, credential=self.credential).rm(path=path)

    def info(self, path: str) -> dict:
        """
        get file properties, such as

        * name
        * creation_time
        * last_modified_time
        * size
        * content_hash(md5)

        Args:
            path:

        Returns:
            dict info of some file
        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_caontainer/test1.csv"
            >>> azc.info(path=path)
            {
                "name": "test1.csv",
                "size": "128KB",
                "creation_time": "",
                "last_modified": "",
                "etag": "etag...",
                "content_type": "",
                "type": "file"
            }

        """
        _, account_kind, _, _ = BlobPathDecoder(path).get_with_url()
        # get info from blob or data-lake storage
        data = AzfsClient.get(account_kind, credential=self.credential).info(path=path)

        # extract below to determine file or directory
        content_settings = data.get("content_settings", {})
        metadata = data.get("metadata", {})

        data_type = ""
        if "hdi_isfolder" in metadata:
            # only data-lake storage has `hdi_isfolder`
            data_type = "directory"
        elif content_settings.get("content_type") is not None:
            # blob and data-lake storage have `content_settings`,
            # and its value of the `content_type` must not be None
            data_type = "file"
        return {
            "name": data.get("name", ""),
            "size": data.get("size", ""),
            "creation_time": data.get("creation_time", ""),
            "last_modified": data.get("last_modified", ""),
            "etag": data.get("etag", ""),
            "content_type": content_settings.get("content_type", ""),
            "type": data_type
        }

    def checksum(self, path: str):
        """
        Blob and DataLake storage have etag.

        Args:
            path:

        Returns:
            etag
        Raises:
            KeyError: if info has no etag

        """
        return self.info(path=path)["etag"]

    def size(self, path):
        """
        Size in bytes of file

        Args:
            path:

        Returns:

        """
        return self.info(path).get("size", None)

    def isdir(self, path):
        """
        Is this entry directory-like?

        Args:
            path:

        Returns:

        """
        try:
            return self.info(path)["type"] == "directory"
        except IOError:
            return False

    def isfile(self, path):
        """
        Is this entry file-like?

        Args:
            path:

        Returns:

        """
        try:
            return self.info(path)["type"] == "file"
        except IOError:
            return False

    def glob(self, pattern_path: str):
        """
        currently only support * wildcard

        Args:
            pattern_path: ex: https://<storage_account_name>.blob.core.windows.net/<container>/*/*.csv

        Returns:

        """
        if "*" not in pattern_path:
            raise AzfsInputError("no any `*` in the `pattern_path`")
        url, account_kind, container_name, file_path = BlobPathDecoder(pattern_path).get_with_url()

        # get container root path
        base_path = f"{url}/{container_name}/"
        file_list = AzfsClient.get(account_kind, credential=self.credential).ls(path=base_path, file_path="")
        if account_kind in ["dfs", "blob"]:
            # fix pattern_path, in order to avoid matching `/`
            pattern_path = rf"{pattern_path.replace('*', '([^/])*?')}$"
            pattern = re.compile(pattern_path)
            file_full_path_list = [f"{base_path}{f}" for f in file_list]
            # filter with pattern.match
            matched_full_path_list = [f for f in file_full_path_list if pattern.match(f)]
            return matched_full_path_list
        elif account_kind in ["queue"]:
            raise NotImplementedError

    def du(self, path):
        pass

    def _get(self, path: str, **kwargs) -> Union[bytes, str, io.BytesIO]:
        """
        storage accountのタイプによってfile_clientを変更し、データを取得する関数
        特定のファイルを取得する関数

        Args:
            path:
            **kwargs:

        Returns:

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_caontainer/test1.csv"
            you can read csv file in azure blob storage
            >>> data = azc.get(path=path)
            `download()` is same method as `get()`
            >>> data = azc.download(path=path)

        """
        _, account_kind, _, _ = BlobPathDecoder(path).get_with_url()
        return AzfsClient.get(account_kind, credential=self.credential).get(path=path, **kwargs)

    def read_csv(self, path: str, **kwargs) -> pd.DataFrame:
        """
        blobにあるcsvを読み込み、pd.DataFrameとして取得する関数。
        gzip圧縮にも対応。

        Args:
            path:
            **kwargs:

        Returns:

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_caontainer/test1.csv"
            you can read and write csv file in azure blob storage
            >>> df = azc.read_csv(path=path)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df = pd.read_csv_az(path)

        """
        file_to_read = self._get(path)
        return pd.read_csv(file_to_read, **kwargs)

    def _put(self, path: str, data) -> bool:
        """
        upload data to blob or data_lake storage account

        Args:
            path:
            data:

        Returns:

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_caontainer/test1.csv"
            you can write file in azure blob storage
            >>> data = azc.put(path=path)
            `download()` is same method as `get()`
            >>> data = azc.upload(path=path)

        """
        _, account_kind, _, _ = BlobPathDecoder(path).get_with_url()
        return AzfsClient.get(account_kind, credential=self.credential).put(path=path, data=data)

    def write_csv(self, path: str, df: pd.DataFrame, **kwargs) -> bool:
        """
        output pandas dataframe to csv file in Datalake storage.

        Args:
            path:
            df:
            **kwargs:

        Returns:
            pd.DataFrame

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_caontainer/test1.csv"
            you can read and write csv file in azure blob storage
            >>> azc.write_csv(path=path, df=df)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df.to_csv_az(path)


        """
        csv_str = df.to_csv(**kwargs).encode("utf-8")
        return self._put(path=path, data=csv_str)

    def read_json(self, path: str, **kwargs) -> dict:
        """
        read json file in Datalake storage.

        Args:
            path:
            **kwargs:

        Returns:
            dict

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_caontainer/test1.json"
            you can read and write csv file in azure blob storage
            >>> azc.read_json(path=path)

        """
        file_bytes = self._get(path)
        if type(file_bytes) is io.BytesIO:
            file_bytes = file_bytes.read()
        return json.loads(file_bytes, **kwargs)

    def write_json(self, path: str, data: dict, **kwargs) -> bool:
        """
        output dict to json file in Datalake storage.

        Args:
            path:
            data:
            **kwargs:

        Returns:
            bool

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_caontainer/test1.json"
            you can read and write csv file in azure blob storage
            >>> azc.write_json(path=path, data={"": ""})

        """
        return self._put(path=path, data=json.dumps(data, **kwargs))

    # ===================
    # alias for functions
    # ===================

    get = _get
    get.__doc__ = _get.__doc__
    download = _get
    download.__doc__ = _get.__doc__
    put = _put
    put.__doc__ = _put.__doc__
    upload = _put
    upload.__doc__ = _put.__doc__
