import bz2
import copy
from functools import partial
import gzip
import io
from inspect import signature
import json
import lzma
import multiprocessing as mp
import pickle
import re
# to accept all typing.*
from typing import *
import warnings

import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError
from azfs.clients import AzfsClient, TextReader
from azfs.error import (
    AzfsInputError,
    AzfsDecoratorFileFormatError,
    AzfsDecoratorSizeNotMatchedError,
    AzfsDecoratorReturnTypeError
)
from azfs.utils import (
    BlobPathDecoder,
    ls_filter
)

__all__ = ["AzFileClient", "ExportDecorator", "export_decorator"]


class ExportDecorator:
    def __init__(self):
        self.functions = []

    def register(self, _as: Optional[str] = None):
        def _wrapper(func: callable):
            func_name = func.__name__
            self.functions.append(
                {
                    "function_name": func_name,
                    "register_as": _as if _as is not None else func_name,
                    "function": func
                }
            )
            return func
        return _wrapper

    __call__ = register


export_decorator = ExportDecorator()


def _wrap_quick_load(inputs: dict):
    """
    read wrapper function for multiprocessing.

    Args:
        inputs:

    Returns:

    """
    return _quick_load(**inputs)


def _quick_load(
        path: str,
        file_format: Optional[str] = None,
        credential: Optional[str] = None,
        apply_method: Optional[callable] = None) -> pd.DataFrame:
    """
    read function for multiprocessing.

    Args:
        path: file-path to read
        file_format: format of the file
        credential:
        apply_method:

    Returns:
        pd.DataFrame
    """
    if credential is None:
        azc = AzFileClient()
    else:
        azc = AzFileClient(credential=credential)

    # set file_format if None
    if file_format is None:
        if path.endswith(".csv"):
            file_format = "csv"
        elif path.endswith(".parquet"):
            file_format = "parquet"
        elif path.endswith(".pkl"):
            file_format = "pickle"
        else:
            raise AzfsInputError("file_format is incorrect")

    # read file as pandas DataFrame
    if file_format == "csv":
        df = azc.read_csv(path=path)
    elif file_format == "parquet":
        df = azc.read_parquet(path=path)
    elif file_format == "pickle":
        df = azc.read_pickle(path=path)
    else:
        raise AzfsInputError("file_format is incorrect")

    # apply additional function
    if apply_method is None:
        return df
    else:
        return apply_method(df)


class DataFrameReader:
    def __init__(
            self,
            _azc,
            credential: Union[str, DefaultAzureCredential],
            path: Union[str, List[str]] = None,
            use_mp=False,
            cpu_count: Optional[int] = None,
            file_format: Optional[str] = None):
        self._azc: AzFileClient = _azc
        # DefaultCredential cannot be pickle (when use multiprocessing), so make it None
        self._credential = credential if type(credential) is str else None
        self.path: Optional[List[str]] = self._decode_path(path=path)
        self.file_format = file_format
        self.use_mp = use_mp
        self.cpu_count = mp.cpu_count() if cpu_count is None else cpu_count
        self._apply_method = None

    def _decode_path(self, path: Optional[Union[str, List[str]]]) -> Optional[List[str]]:
        """
        decode path to be read by azc

        Args:
            path: azure blob path

        Returns:

        """
        if path is None:
            return None
        elif type(path) is str:
            if "*" in path:
                decoded_path = self._azc.glob(pattern_path=path)
            else:
                decoded_path = [path]
        elif type(path) is list:
            decoded_path = path
        else:
            raise AzfsInputError("path must be `str` or `list`")
        return decoded_path

    def csv(self, path: Union[str, List[str]] = None, **kwargs) -> pd.DataFrame:
        """
        read csv files in Azure Blob, like PySpark-method.

        Args:
            path: azure blob path
            **kwargs: as same as pandas.read_csv

        Returns:
            pd.DataFrame

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> blob_path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            >>> df = azc.read().csv(blob_path)
            # result is as same as azc.read_csv(blob_path)
            >>> blob_path_list = [
            ...     "https://testazfs.blob.core.windows.net/test_container/test1.csv",
            ...     "https://testazfs.blob.core.windows.net/test_container/test2.csv"
            ... ]
            >>> df = azc.read().csv(blob_path_list)
            # result is as same as pd.concat([each data-frame])
            # in addition, you can use `*`
            >>> blob_path_pattern = "https://testazfs.blob.core.windows.net/test_container/test*.csv"
            >>> df = azc.read().csv(blob_path_pattern)
            # you can use multiprocessing with `use_mp` argument
            >>> df = azc.read(use_mp=True).csv(blob_path_pattern)
            # if you want to filter or apply some method, you can use your defined function as below
            >>> def filter_function(_df: pd.DataFrame, _id: str) -> pd.DataFrame:
            ...     return _df[_df['id'] == _id]
            >>> df = azc.read(use_mp=True).apply(function=filter_function, _id="aaa").csv(blob_path_pattern)


        """
        self.file_format = "csv"
        if path is not None:
            self.path = self._decode_path(path=path)
        return self._load(**kwargs)

    def parquet(self, path: Union[str, List[str]] = None) -> pd.DataFrame:
        """
        read parquet files in Azure Blob, like PySpark-method.

        Args:
            path: azure blob path

        Returns:
            pd.DataFrame

        """
        self.file_format = "parquet"
        if path is not None:
            self.path = self._decode_path(path=path)
        return self._load()

    def pickle(self, path: Union[str, List[str]] = None, compression: str = "gzip") -> pd.DataFrame:
        """
        read pickle files in Azure Blob, like PySpark-method.

        Args:
            path: azure blob path
            compression: acceptable keywords are: gzip, bz2, xz. gzip is default value.

        Returns:
            pd.DataFrame

        """
        self.file_format = "pickle"
        if path is not None:
            self.path = self._decode_path(path=path)
        return self._load(compression=compression)

    def _load_function(self) -> callable:
        """
        get read_* function according to the file_format

        Returns:

        """
        if self.file_format == "csv":
            load_function = self._azc.read_csv
        elif self.file_format == "parquet":
            load_function = self._azc.read_parquet
        elif self.file_format == "pickle":
            load_function = self._azc.read_pickle
        else:
            raise AzfsInputError("file_format is incorrect")
        return load_function

    def apply(self, *, function: callable, **kwargs):
        """
        to apply pandas DataFrame

        Args:
            function: first argument must pass pd.DataFrame
            **kwargs: argument to pass the function

        Returns:
            self
        """
        if kwargs:
            self._apply_method = partial(function, **kwargs)
        else:
            self._apply_method = function
        return self

    def _load(self, **kwargs) -> Optional[pd.DataFrame]:
        if self.path is None:
            raise AzfsInputError("input azure blob path")

        if self.use_mp:
            params_list = []
            for f in self.path:
                _input = {
                    "path": f,
                    "file_format": self.file_format,
                    "credential": self._credential,
                    "apply_method": self._apply_method
                }
                _input.update(kwargs)
                params_list.append(_input)
            with mp.Pool(self.cpu_count) as pool:
                df_list = pool.map(_wrap_quick_load, params_list)
            pool.join()
        else:
            load_function = self._load_function()
            if self._apply_method is None:
                df_list = [load_function(f, **kwargs) for f in self.path]
            else:
                df_list = [self._apply_method(load_function(f, **kwargs)) for f in self.path]
        if len(df_list) == 0:
            return None
        return pd.concat(df_list)


class AzFileClient:
    """

    AzFileClient is

    * list files in blob (also with wildcard ``*``),
    * check if file exists,
    * read csv as pd.DataFrame, and json as dict from blob,
    * write pd.DataFrame as csv, and dict as json to blob,

    Examples:
        >>> import azfs
        >>> from azure.identity import DefaultAzureCredential
        credential is not required if your environment is on AAD
        >>> azc = azfs.AzFileClient()
        credential is required if your environment is not on AAD
        >>> credential = "[your storage account credential]"
        >>> azc = azfs.AzFileClient(credential=credential)
        # or
        >>> credential = DefaultAzureCredential()
        >>> azc = azfs.AzFileClient(credential=credential)
        connection_string will be also acceptted
        >>> connection_string = "[your connection_string]"
        >>> azc = azfs.AzFileClient(connection_string=connection_string)
    """

    class AzContextManager:
        """
        AzContextManger provides easy way to set new function as attribute to another package like pandas.
        """
        def __init__(self):
            self.register_list = []

        def register(self, _as: str, _to: object):
            """
            register decorated function to self.register_list.


            Args:
                _as: new method name
                _to: assign to class or object

            Returns:
                decorated function

            """
            def _register(function):
                """
                append ``wrapper`` function

                Args:
                    function:

                Returns:

                """
                def wrapper(class_instance):
                    """
                    accept instance in kwargs as name of ``az_file_client_instance``

                    Args:
                        class_instance: always instance of AzFileClient

                    Returns:

                    """

                    def new_function(*args, **kwargs):
                        """
                        actual wrapped function

                        Args:
                            *args:
                            **kwargs:

                        Returns:

                        """
                        target_function = getattr(class_instance, function.__name__)

                        df = args[0] if isinstance(args[0], pd.DataFrame) else None
                        if df is not None:
                            kwargs['df'] = args[0]
                            return target_function(*args[1:], **kwargs)
                        return target_function(*args, **kwargs)

                    return new_function

                function_info = {
                    "assign_as": _as,
                    "assign_to": _to,
                    "function": wrapper
                }
                self.register_list.append(function_info)

                return function

            return _register

        def attach(self, client: object):
            """
            set new function as attribute based on self.register_list

            Args:
                client: set AzFileClient always

            Returns:
                None

            """
            for f in self.register_list:
                setattr(f['assign_to'], f['assign_as'], f['function'](class_instance=client))

        def detach(self):
            """
            set None based on self.register_list

            Returns:
                None

            """
            for f in self.register_list:
                setattr(f['assign_to'], f['assign_as'], None)

    # instance for context manager
    _az_context_manager = AzContextManager()

    def __init__(
            self,
            credential: Optional[Union[str, DefaultAzureCredential]] = None,
            connection_string: Optional[str] = None):
        """
        if every argument is None, set credential as DefaultAzureCredential().

        Args:
            credential: if string, Blob Storage -> Access Keys -> Key
            connection_string: connection_string
        """

        if credential is None and connection_string is None:
            credential = DefaultAzureCredential()
        self._client = AzfsClient(credential=credential, connection_string=connection_string)
        self._credential = credential

    def __enter__(self):
        """
        add some functions to pandas module based on AzContextManger()

        Returns:
            instance of AzFileClient

        """
        self._az_context_manager.attach(client=self)
        return self

    def __exit__(self, exec_type, exec_value, traceback):
        """
        remove some functions from pandas module based on AzContextManager()

        Args:
            exec_type:
            exec_value:
            traceback:

        Returns:
            None
        """
        self._az_context_manager.detach()

    def exists(self, path: str) -> bool:
        """
        check if specified file exists or not.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.csv``

        Returns:
            ``True`` if files exists, otherwise ``False``

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> csv_path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            >>> azc.exists(path=csv_path)
            True
            >>> csv_path = "https://testazfs.blob.core.windows.net/test_container/not_exist_test1.csv"
            >>> azc.exists(path=csv_path)
            False

        """
        try:
            _ = self.info(path=path)
        except ResourceNotFoundError:
            return False
        else:
            return True

    def ls(self, path: str, attach_prefix: bool = False) -> list:
        """
        list blob file from blob or dfs.

        Args:
            path: Azure Blob path URL format, ex: https://testazfs.blob.core.windows.net/test_container
            attach_prefix: return full_path if True, return only name

        Returns:
            list of azure blob files

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> csv_path = "https://testazfs.blob.core.windows.net/test_container"
            >>> azc.ls(csv_path)
            [
                "test1.csv",
                "test2.csv",
                "test3.csv",
                "directory_1",
                "directory_2"
            ]
            >>> azc.ls(path=path, attach_prefix=True)
            [
                "https://testazfs.blob.core.windows.net/test_container/test1.csv",
                "https://testazfs.blob.core.windows.net/test_container/test2.csv",
                "https://testazfs.blob.core.windows.net/test_container/test3.csv",
                "https://testazfs.blob.core.windows.net/test_container/directory_1",
                "https://testazfs.blob.core.windows.net/test_container/directory_2"
            ]

        """
        _, account_kind, _, file_path = BlobPathDecoder(path).get_with_url()
        file_list = self._client.get_client(account_kind=account_kind).ls(path=path, file_path=file_path)
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

    def cp(self, src_path: str, dst_path: str, overwrite=False) -> bool:
        """
        copy the data from `src_path` to `dst_path`

        Args:
            src_path:
                Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.csv``
            dst_path:
                Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test2.csv``
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

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.csv``

        Returns:
            True if target file is correctly removed.

        """
        _, account_kind, _, _ = BlobPathDecoder(path).get_with_url()
        return self._client.get_client(account_kind=account_kind).rm(path=path)

    def info(self, path: str) -> dict:
        """
        get file properties, such as
        ``name``,  ``creation_time``, ``last_modified_time``, ``size``, ``content_hash(md5)``.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.csv``

        Returns:
            dict info of some file

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> csv_path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            >>> azc.info(path=csv_path)
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
        data = self._client.get_client(account_kind=account_kind).info(path=path)

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

    def checksum(self, path: str) -> str:
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

    def size(self, path) -> Optional[Union[int, str]]:
        """
        Size in bytes of file

        Args:
            path:

        Returns:

        """
        return self.info(path).get("size")

    def isdir(self, path) -> bool:
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

    def isfile(self, path) -> bool:
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

    def glob(self, pattern_path: str) -> List[str]:
        """
        Currently only support ``* (wildcard)`` .
        By default, ``glob()`` lists specified files with formatted-URL.

        Args:
            pattern_path: ex: ``https://<storage_account_name>.blob.core.windows.net/<container>/*/*.csv``

        Returns:
            lists specified files filtered by wildcard

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_container/some_folder"
            ls() lists all files in some folder like
            >>> azc.ls(path)
            [
                "test1.csv",
                "test2.csv",
                "test3.csv",
                "test1.json",
                "test2.json",
                "directory_1",
                "directory_2"
            ]
            glob() lists specified files according to the wildcard, and lists with formatted-URL by default
            >>> csv_pattern_path = "https://testazfs.blob.core.windows.net/test_container/some_folder/*.csv"
            >>> azc.glob(path=csv_pattern_path)
            [
                "https://testazfs.blob.core.windows.net/test_container/some_folder/test1.csv",
                "https://testazfs.blob.core.windows.net/test_container/some_folder/test2.csv",
                "https://testazfs.blob.core.windows.net/test_container/some_folder/test3.csv"
            ]
            glob() can use any path
            >>> csv_pattern_path = "https://testazfs.blob.core.windows.net/test_container/some_folder/test1.*"
            >>> azc.glob(path=csv_pattern_path)
            [
                "https://testazfs.blob.core.windows.net/test_container/some_folder/test1.csv",
                "https://testazfs.blob.core.windows.net/test_container/some_folder/test1.json"
            ]
            also deeper folders
            >>> csv_pattern_path = "https://testazfs.blob.core.windows.net/test_container/some_folder/*/*.csv"
            >>> azc.glob(path=csv_pattern_path)
            [
                "https://testazfs.blob.core.windows.net/test_container/some_folder/directory_1/deeper_test1.csv",
                "https://testazfs.blob.core.windows.net/test_container/some_folder/directory_2/deeper_test2.csv"
            ]

        Raises:
            AzfsInputError: when ``*`` is used in root_flder under a container.
        """
        if "*" not in pattern_path:
            raise AzfsInputError("no any `*` in the `pattern_path`")
        url, account_kind, container_name, file_path = BlobPathDecoder(pattern_path).get_with_url()

        acceptable_folder_pattern = r"(?P<root_folder>[^\*]+)/(?P<folders>.*)"
        result = re.match(acceptable_folder_pattern, file_path)
        if result:
            result_dict = result.groupdict()
            root_folder = result_dict['root_folder']
        else:
            raise AzfsInputError(
                f"Cannot use `*` in root_folder under a container. Accepted format is {acceptable_folder_pattern}"
            )
        # get container root path
        base_path = f"{url}/{container_name}/"
        if account_kind in ["dfs", "blob"]:
            file_list = self._client.get_client(account_kind=account_kind).ls(path=base_path, file_path=root_folder)

            # to escape special chars for regular-expression
            def _escape(input_str: str) -> str:
                special_chars = ["(", ")", "[", "]"]
                for c in special_chars:
                    input_str = input_str.replace(c, f"\\{c}")
                return input_str

            escaped_pattern_path = _escape(pattern_path)
            # fix pattern_path, in order to avoid matching `/`
            replace_pattern_path = escaped_pattern_path.replace('*', '([^/])*?')
            pattern = re.compile(f"{replace_pattern_path}$")
            file_full_path_list = [f"{base_path}{f}" for f in file_list]
            # filter with pattern.match
            matched_full_path_list = [f for f in file_full_path_list if pattern.match(f)]
            return matched_full_path_list
        elif account_kind in ["queue"]:
            raise NotImplementedError

    def read(
            self,
            *,
            path: Union[str, List[str]] = None,
            use_mp: bool = False,
            cpu_count: Optional[int] = None,
            file_format: str = "csv") -> DataFrameReader:
        """
        read csv, parquet, picke files in Azure Blob, like PySpark-method.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.csv``
            use_mp: Default, False
            cpu_count: Default, as same as mp.cpu_count()
            file_format: determined by which function you call

        Returns:
            pd.DataFrame

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> blob_path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            >>> df = azc.read().csv(blob_path)
            # result is as same as azc.read_csv(blob_path)
            >>> blob_path_list = [
            ...     "https://testazfs.blob.core.windows.net/test_container/test1.csv",
            ...     "https://testazfs.blob.core.windows.net/test_container/test2.csv"
            ... ]
            >>> df = azc.read().csv(blob_path_list)
            # result is as same as pd.concat([each data-frame])
            # in addition, you can use `*`
            >>> blob_path_pattern = "https://testazfs.blob.core.windows.net/test_container/test*.csv"
            >>> df = azc.read().csv(blob_path_pattern)
            # you can use multiprocessing with `use_mp` argument
            >>> df = azc.read(use_mp=True).csv(blob_path_pattern)
            # if you want to filter or apply some method, you can use your defined function as below
            >>> def filter_function(_df: pd.DataFrame, _id: str) -> pd.DataFrame:
            ...     return _df[_df['id'] == _id]
            >>> df = azc.read(use_mp=True).apply(function=filter_function, _id="aaa").csv(blob_path_pattern)


        """
        return DataFrameReader(
            _azc=self,
            credential=self._credential,
            path=path,
            use_mp=use_mp,
            cpu_count=cpu_count,
            file_format=file_format)

    def _get(self, path: str, offset: int = None, length: int = None, **kwargs) -> Union[bytes, str, io.BytesIO, dict]:
        """
        get data from Azure Blob Storage.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.csv``
            offset:
            length:
            **kwargs:

        Returns:
            some data

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> csv_path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            you can read csv file in azure blob storage
            >>> data = azc.get(path=csv_path)
            `download()` is same method as `get()`
            >>> data = azc.download(path=csv_path)

        """
        _, account_kind, _, _ = BlobPathDecoder(path).get_with_url()

        file_bytes = self._client.get_client(
            account_kind=account_kind).get(path=path, offset=offset, length=length, **kwargs)
        # gzip圧縮ファイルは一旦ここで展開
        if path.endswith(".gz"):
            file_bytes = gzip.decompress(file_bytes)

        if type(file_bytes) is bytes:
            file_to_read = io.BytesIO(file_bytes)
        else:
            file_to_read = file_bytes

        return file_to_read

    def read_line_iter(self, path: str) -> iter:
        """
        To read text file in each line with iterator.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.csv``

        Returns:
            get data of the path as iterator

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> csv_path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            >>> for l in azc.read_line_iter(path=csv_path)
            ...     print(l.decode("utf-8"))

        """
        _, account_kind, _, _ = BlobPathDecoder(path).get_with_url()
        return TextReader(client=self._client.get_client(account_kind=account_kind), path=path)

    def read_csv_chunk(self, path: str, chunk_size: int) -> pd.DataFrame:
        """
        !WARNING! the method may differ from current version in the future update.
        Currently, only support for csv.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.csv``
            chunk_size: pandas-DataFrame index length to read.

        Returns:
            first time: len(df.index) is `chunk_size - 1`
            second time or later: len(df.index) is `chunk_size`

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> csv_path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            >>> read_chunk_size = 100
            >>> for _df in azc.read_csv_chunk(path=csv_path, chunk_size=read_chunk_size):
            ...   print(_df)
        """
        warning_message = """
            The method is under developing. 
            The name or the arguments may differ from current version in the future update.
        """
        warnings.warn(warning_message, FutureWarning)
        initial_line = ""
        byte_list = []

        for idx, l in enumerate(self.read_line_iter(path=path)):
            div_idx = idx % chunk_size
            if idx == 0:
                initial_line = l
                byte_list.append(initial_line)
            else:
                byte_list.append(l)
            if div_idx + 1 == chunk_size:
                file_to_read = (b"\n".join(byte_list))
                file_to_io_read = io.BytesIO(file_to_read)
                df = pd.read_csv(file_to_io_read)
                yield df

                byte_list = [initial_line]
        # make remainder DataFrame after the for-loop
        file_to_read = (b"\n".join(byte_list))
        file_to_io_read = io.BytesIO(file_to_read)
        df = pd.read_csv(file_to_io_read)
        yield df

    @_az_context_manager.register(_as="read_csv_az", _to=pd)
    def read_csv(self, path: str, **kwargs) -> pd.DataFrame:
        """
        get csv data as pd.DataFrame from Azure Blob Storage.
        support ``csv`` and also ``csv.gz``.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.csv``
            **kwargs: keywords to put df.read_csv(), such as ``header``, ``encoding``.

        Returns:
            pd.DataFrame

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> csv_path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            you can read and write csv file in azure blob storage
            >>> df = azc.read_csv(path=csv_path)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df = pd.read_csv_az(path)

        """
        file_to_read = self._get(path)
        return pd.read_csv(file_to_read, **kwargs)

    @_az_context_manager.register(_as="read_table_az", _to=pd)
    def read_table(self, path: str, **kwargs) -> pd.DataFrame:
        """
        get tsv data as pd.DataFrame from Azure Blob Storage.
        support ``tsv``.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.tsv``
            **kwargs: keywords to put df.read_csv(), such as ``header``, ``encoding``.

        Returns:
            pd.DataFrame

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> tsv_path = "https://testazfs.blob.core.windows.net/test_container/test1.tsv"
            you can read and write csv file in azure blob storage
            >>> df = azc.read_table(path=tsv_path)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df = pd.read_table_az(tsv_path)

        """
        file_to_read = self._get(path)
        return pd.read_table(file_to_read, **kwargs)

    @_az_context_manager.register(_as="read_pickle_az", _to=pd)
    def read_pickle(self, path: str, compression="gzip") -> pd.DataFrame:
        """
        get pickled-pandas data as pd.DataFrame from Azure Blob Storage.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.pkl``
            compression: acceptable keywords are: gzip, bz2, xz. gzip is default value.

        Returns:
            pd.DataFrame

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> pkl_path = "https://testazfs.blob.core.windows.net/test_container/test1.pkl"
            you can read and write csv file in azure blob storage
            >>> df = azc.read_pickle(path=pkl_path)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df = pd.read_pickle_az(pkl_path)
            you can use difference compression
            >>> with azc:
            >>>     df = pd.read_pickle_az(pkl_path, compression="bz2")

        """
        file_to_read = self._get(path).read()
        if compression == "gzip":
            file_to_read = gzip.decompress(file_to_read)
        elif compression == "bz2":
            file_to_read = bz2.decompress(file_to_read)
        elif compression == "xz":
            file_to_read = lzma.decompress(file_to_read)
        return pd.DataFrame(pickle.loads(file_to_read))

    @_az_context_manager.register(_as="read_parquet_az", _to=pd)
    def read_parquet(self, path: str) -> pd.DataFrame:
        """

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test.parquet``

        Returns:
            pd.DataFrame

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> parquet_path = "https://testazfs.blob.core.windows.net/test_container/test1.parquet"
            you can read and write csv file in azure blob storage
            >>> df = azc.read_parquet(path=parquet_path)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df = pd.read_parquet_az(parquet_path)


        """
        import pyarrow.parquet as pq
        data = self._get(path=path)
        return pq.read_table(data).to_pandas()

    def _put(self, path: str, data) -> bool:
        """
        upload data to blob or data_lake storage.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.csv``
            data: some data to upload.

        Returns:
            True if correctly uploaded

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> csv_path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            you can write file in azure blob storage
            >>> _data = azc.put(path=csv_path)
            `download()` is same method as `get()`
            >>> _data = azc.upload(path=csv_path)

        """
        _, account_kind, _, _ = BlobPathDecoder(path).get_with_url()
        return self._client.get_client(account_kind=account_kind).put(path=path, data=data)

    @_az_context_manager.register(_as="to_csv_az", _to=pd.DataFrame)
    def write_csv(self, path: str, df: pd.DataFrame, **kwargs) -> bool:
        """
        output pandas dataframe to csv file in Datalake storage.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.csv``.
            df: pd.DataFrame to upload.
            **kwargs: keywords to put df.to_csv(), such as ``encoding``, ``index``.

        Returns:
            True if correctly uploaded

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> csv_path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            you can read and write csv file in azure blob storage
            >>> azc.write_csv(path=csv_path, df=df)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df.to_csv_az(csv_path)
        """
        csv_str = df.to_csv(**kwargs).encode("utf-8")
        return self._put(path=path, data=csv_str)

    @_az_context_manager.register(_as="to_table_az", _to=pd.DataFrame)
    def write_table(self, path: str, df: pd.DataFrame, **kwargs) -> bool:
        """
        output pandas dataframe to tsv file in Datalake storage.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.tsv``.
            df: pd.DataFrame to upload.
            **kwargs: keywords to put df.to_csv(), such as ``encoding``, ``index``.

        Returns:
            True if correctly uploaded

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> tsv_path = "https://testazfs.blob.core.windows.net/test_container/test1.tsv"
            you can read and write csv file in azure blob storage
            >>> azc.write_table(path=tsv_path, df=df)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df.to_table_az(tsv_path)
        """
        table_str = df.to_csv(sep="\t", **kwargs).encode("utf-8")
        return self._put(path=path, data=table_str)

    @_az_context_manager.register(_as="to_pickle_az", _to=pd.DataFrame)
    def write_pickle(self, path: str, df: pd.DataFrame, compression="gzip") -> bool:
        """
        output pandas dataframe to tsv file in Datalake storage.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.pkl``
            df: pd.DataFrame to upload.
            compression: acceptable keywords are: gzip, bz2, xz. gzip is default value.

        Returns:
            pd.DataFrame

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> pkl_path = "https://testazfs.blob.core.windows.net/test_container/test1.pkl"
            you can read and write csv file in azure blob storage
            >>> azc.write_pickle(path=pkl_path, df=df)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df.to_pickle_az(pkl_path)
            you can use difference compression
            >>> with azc:
            >>>     df.to_pickle_az(pkl_path, compression="bz2")

        """
        serialized_data = pickle.dumps(df)
        if compression == "gzip":
            serialized_data = gzip.compress(serialized_data)
        elif compression == "bz2":
            serialized_data = bz2.compress(serialized_data)
        elif compression == "xz":
            serialized_data = lzma.compress(serialized_data)
        return self._put(path=path, data=serialized_data)

    @_az_context_manager.register(_as="to_parquet_az", _to=pd.DataFrame)
    def write_parquet(self, path: str, table) -> bool:
        """
        When implementation of AzFileSystem is done, the function will be implemented.


        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test.parquet``
            table: parquet table

        Returns:
            True: if successfully uploaded

        Examples:
            >>> from azfs import AzFileSystem
            >>> import pyarrow.parquet as pq
            >>> fs = AzFileSystem()
            >>> with fs.open("azure_path", "wb") as f:
            ...     pq.write_table(table, f)

        """
        raise NotImplementedError

    def read_json(self, path: str, **kwargs) -> dict:
        """
        read json file in Datalake storage.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.json``
            **kwargs: keywords to put json.loads(), such as ``parse_float``.

        Returns:
            dict

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> json_path = "https://testazfs.blob.core.windows.net/test_container/test1.json"
            you can read and write csv file in azure blob storage
            >>> azc.read_json(path=json_path)

        """
        file_bytes = self._get(path)
        if type(file_bytes) is io.BytesIO:
            file_bytes = file_bytes.read()
        return json.loads(file_bytes, **kwargs)

    def write_json(self, path: str, data: dict, **kwargs) -> bool:
        """
        output dict to json file in Datalake storage.

        Args:
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.json``
            data: dict to upload
            **kwargs: keywords to put json.loads(), such as ``indent``.

        Returns:
            True if correctly uploaded

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> json_path = "https://testazfs.blob.core.windows.net/test_container/test1.json"
            you can read and write csv file in azure blob storage
            >>> azc.write_json(path=json_path, data={"": ""})

        """
        # encode with UTF-8 to fully upload data including not ascii character
        return self._put(path=path, data=json.dumps(data, **kwargs).encode("utf-8"))

    # import decorator
    def import_decorator(
            self,
            decorator: ExportDecorator,
            *,
            keyword_list: list,
            storage_account: Optional[Union[str, dict]] = None,
            storage_type: Union[str, dict] = "blob",
            container: Optional[Union[str, dict]] = None,
            key: Optional[Union[str, dict]] = None,
            output_parent_path: Optional[Union[str, dict]] = None,
            file_name_prefix: Optional[Union[str, dict]] = None,
            file_name: Optional[Union[str, dict]] = None,
            file_name_suffix: Optional[Union[str, dict]] = None,
            export: Union[bool, dict] = True,
            format_type: Union[str, dict] = "csv",
            **write_kwargs
    ):
        """
        set user-defined functions as attribute of azfs.AzFileClient.


        Args:
            decorator:
            keyword_list:
            storage_account:
            storage_type:
            container:
            key:
            output_parent_path:
            file_name_prefix:
            file_name:
            file_name_suffix:
            export:
            format_type:
            write_kwargs: additional default parameters, ex. to_csv(**write_kwargs)

        Returns:
            None

        Examples:
            >>> import azfs
            >>> from azfs import export_decorator
            # define your function with the decorator
            >>> @export_decorator.register()
            >>> def your_function(name) -> pd.DataFrame:
            >>>     return pd.DataFrame()
            # import the defined function
            >>> azc = azfs.AzFileClient()
            >>> azc.import_decorator(
            ...     decorator=your_decorator,
            ...     keyword_list=["prod"],
            ...     output_parent_path="https://your_storage_account.../your_container/your_folder",
            ... )
            # then you can save your pd.DataFrame.
            >>> azc.your_function(name="your_name", _prod_file_name="example")


        """
        for func_dict in decorator.functions:
            original_func_name = func_dict['function_name']
            func_name = func_dict['register_as']
            func = func_dict['function']

            def _decode(
                    kwrd: str,
                    suffix: str,
                    kwargs_import_function: Optional[Union[str, dict]],
                    kwargs_invoke_function: dict) -> Optional[Union[str, bool]]:
                """

                Args:
                    kwrd: one item in `keyword_list`
                    suffix:
                    kwargs_import_function: argument from `azfs.AzFileClient::import_decorator`
                    kwargs_invoke_function: argument from user-defined function
                """
                keyword = f"_{kwrd}_{suffix}"
                target_value_from_invoke_function = kwargs_invoke_function.pop(keyword, None)
                if target_value_from_invoke_function is not None:
                    return target_value_from_invoke_function
                if f"_{suffix}" in kwargs_invoke_function:
                    return kwargs_invoke_function[f"_{suffix}"]

                if kwargs_import_function is None:
                    return None
                if type(kwargs_import_function) is str \
                        or type(kwargs_import_function) is bool \
                        or type(kwargs_import_function) is list:
                    return kwargs_import_function
                elif type(kwargs_import_function) is dict:
                    return kwargs_import_function.pop(kwrd, None)
                else:
                    raise ValueError("type not matched.")

            def _wrapper(
                    _func: callable,
            ):

                def _actual_function(*args, **kwargs):
                    """
                    do the things below:
                        1. get additional argument with `dict.pop`
                        2. attach additional prefix/suffix to `file_name`
                        3. append output candidate paths
                        4. pick the parameter based on user-defined function and call it
                        5. get the return value of the user-defined function as pd.DataFrame
                        6. save the pd.DataFrame to the paths

                    Args:
                        *args:
                        **kwargs:

                    Returns:
                        pd.DataFrame as same as user-defined function
                    """
                    # default keyword arguments for `to_csv`, etc
                    write_kwargs_ = copy.deepcopy(write_kwargs)
                    # output_path_list
                    output_path_list = []
                    for keyword in keyword_list:
                        storage_account_: str = _decode(
                            keyword, "storage_account", copy.deepcopy(storage_account), kwargs)
                        storage_type_: str = _decode(
                            keyword, "storage_type", copy.deepcopy(storage_type), kwargs)
                        container_: str = _decode(
                            keyword, "container", copy.deepcopy(container), kwargs)
                        key_: str = _decode(
                            keyword, "key", copy.deepcopy(key), kwargs)
                        output_parent_path_: str = _decode(
                            keyword, "output_parent_path", copy.deepcopy(output_parent_path), kwargs)
                        file_name_prefix_: str = _decode(
                            keyword, "file_name_prefix", copy.deepcopy(file_name_prefix), kwargs)
                        file_name_: Union[str, list] = _decode(
                            keyword, "file_name", copy.deepcopy(file_name), kwargs)
                        file_name_suffix_: str = _decode(
                            keyword, "file_name_suffix", copy.deepcopy(file_name_suffix), kwargs)
                        export_: bool = _decode(
                            keyword, "export", copy.deepcopy(export), kwargs)
                        format_type_: bool = _decode(
                            keyword, "format_type", copy.deepcopy(format_type), kwargs)

                        # add prefix
                        if file_name_prefix_ is not None:
                            if type(file_name_) is str:
                                file_name_ = f"{file_name_prefix_}{file_name_}"
                            elif type(file_name_) is list:
                                file_name_ = [f"{file_name_prefix_}{f}" for f in file_name_]
                        # add suffix
                        if file_name_suffix_ is not None:
                            if type(file_name_) is str:
                                file_name_ = f"{file_name_}{file_name_suffix_}"
                            elif type(file_name_) is list:
                                file_name_ = [f"{f}{file_name_suffix_}" for f in file_name_]

                        if export_:
                            if output_parent_path_ is not None and file_name_ is not None:
                                if key_ is not None:
                                    if type(file_name_) is str:
                                        output_path_list.append(
                                            f"{output_parent_path_}/{key_}/{file_name_}.{format_type_}")
                                    elif type(file_name_) is list:
                                        output_path_list.append(
                                            [f"{output_parent_path_}/{key_}/{f}.{format_type_}" for f in file_name_]
                                        )
                                else:
                                    if type(file_name_) is str:
                                        output_path_list.append(
                                            f"{output_parent_path_}/{file_name_}.{format_type_}")
                                    elif type(file_name_) is list:
                                        output_path_list.append(
                                            [f"{output_parent_path_}/{f}.{format_type_}" for f in file_name_]
                                        )

                            elif storage_account_ is not None and \
                                    storage_type_ is not None and \
                                    container_ is not None and \
                                    file_name_ is not None:
                                if key_ is not None:
                                    url_ = f"https://{storage_account_}.{storage_type_}.core.windows.net"
                                    if type(file_name_) is str:
                                        output_path_list.append(
                                            f"{url_}/{container_}/{key_}/{file_name_}.{format_type_}")
                                    elif type(file_name_) is list:
                                        output_path_list.append(
                                            [f"{url_}/{container_}/{key_}/{f}.{format_type_}" for f in file_name_]
                                        )
                                else:
                                    url_ = f"https://{storage_account_}.{storage_type_}.core.windows.net"
                                    if type(file_name_) is str:
                                        output_path_list.append(
                                            f"{url_}/{container_}/{file_name_}.{format_type_}")
                                    elif type(file_name_) is list:
                                        output_path_list.append(
                                            [f"{url_}/{container_}/{f}.{format_type_}" for f in file_name_]
                                        )

                    # check the argument for the `_func`, and replace only `keyword arguments`
                    sig = signature(_func)
                    kwargs_for_func = {}
                    for signature_params in sig.parameters:
                        if signature_params in kwargs:
                            kwargs_for_func.update({signature_params: kwargs.pop(signature_params)})

                    # get return of the `_func`
                    _df = _func(*args, **kwargs_for_func)

                    # pop unused kwargs for `to_csv` or `to_pickle`
                    pop_keyword_list = [
                        "_storage_account",
                        "_storage_type",
                        "_container",
                        "_key",
                        "_output_parent_path",
                        "_file_name_prefix",
                        "_file_name",
                        "_file_name_suffix",
                        "_export",
                        "_file_format"
                    ]
                    for pop_keyword in pop_keyword_list:
                        _ = kwargs.pop(pop_keyword, None)

                    # additional kwargs for `to_csv` or `to_pickle`
                    write_kwargs_.update(kwargs)
                    if type(_df) is pd.DataFrame:
                        # single dataframe
                        for output_path in output_path_list:
                            if output_path.endswith("csv"):
                                self.write_csv(path=output_path, df=_df, **write_kwargs_)
                            elif output_path.endswith("pickle"):
                                self.write_pickle(path=output_path, df=_df, **write_kwargs_)
                            else:
                                raise AzfsDecoratorFileFormatError()
                    elif type(_df) is tuple:
                        # multiple dataframe
                        for output_path in output_path_list:
                            if len(output_path) != len(_df):
                                raise AzfsDecoratorSizeNotMatchedError()
                            for i_df, i_output_path in zip(_df, output_path):
                                if type(i_df) is not pd.DataFrame:
                                    raise AzfsDecoratorReturnTypeError()
                                if i_output_path.endswith("csv"):
                                    self.write_csv(path=i_output_path, df=i_df, **kwargs)
                                elif i_output_path.endswith("pickle"):
                                    self.write_pickle(path=i_output_path, df=i_df, **kwargs)
                                else:
                                    raise AzfsDecoratorFileFormatError()

                    else:
                        raise AzfsDecoratorReturnTypeError()
                    return _df
                return _actual_function

            def _generate_parameter_args(additional_args: str) -> str:
                """

                Args:
                    additional_args:

                Returns:
                    argument example for the function
                """
                args_list = [
                    f"\n        == params for {additional_args} ==",
                    f"\n        {additional_args}_storage_account: (str) storage account, default:={storage_account}",
                    f"\n        {additional_args}_storage_type: (str) `blob` or `dfs`, default:={storage_type}",
                    f"\n        {additional_args}_container: (str) container, default:={container}",
                    f"\n        {additional_args}_key: (str) folder path, default:={key}",
                    f"\n        {additional_args}_output_parent_path: (str) parent path, default:={output_parent_path}",
                    f"\n        {additional_args}_file_name_prefix: (str) file name prefix, default:={file_name_prefix}"
                    f"\n        {additional_args}_file_name: (str, list) file name, default:={file_name}"
                    f"\n        {additional_args}_file_name_suffix: (str) file name suffix, default:={file_name_suffix}"
                    f"\n        {additional_args}_export: (bool) export if True, default:={export}"
                    f"\n        {additional_args}_format_type: (str) file format, default:={format_type}"
                ]
                return "".join(args_list)

            def _append_docs(docstring: Optional[str], additional_args_list: list) -> str:
                """
                append/generate docstring

                Args:
                    docstring: already written docstring
                    additional_args_list:

                Returns:
                    `docstring`
                """
                result_list = []
                if docstring is not None:
                    for s in docstring.split("\n\n"):
                        if "Args:" in s:
                            args_list = [_generate_parameter_args(arg) for arg in additional_args_list]
                            addition_s = f"{s}{''.join(args_list)}"
                            result_list.append(addition_s)
                        else:
                            result_list.append(s)
                    return "\n\n".join(result_list)
                else:
                    result_list.append(f"original_func_name:= {original_func_name}")
                    result_list.append("Args:")
                    args_list = [_generate_parameter_args(arg) for arg in additional_args_list]
                    addition_s = ''.join(args_list)
                    result_list.append(addition_s)
                    return "\n\n".join(result_list)

            # mutable object is to Null, after initial reference
            wrapped_function = _wrapper(
                _func=func,
            )
            wrapped_function.__doc__ = _append_docs(func.__doc__, additional_args_list=keyword_list)
            if func_name in self.__dict__.keys():
                warnings.warn(f"function name `{func_name}` is already given.")
            setattr(self, func_name, wrapped_function)

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

    # end of the main file
