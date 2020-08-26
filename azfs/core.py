import bz2
import gzip
import io
import json
import lzma
import pickle
import re
from typing import Union, Optional, List
import warnings
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError
from azfs.clients import AzfsClient, TextReader
from azfs.error import AzfsInputError
from azfs.utils import (
    BlobPathDecoder,
    ls_filter
)


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
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            >>> azc.exists(path=path)
            True
            >>> path = "https://testazfs.blob.core.windows.net/test_container/not_exist_test1.csv"
            >>> azc.exists(path=path)
            False

        """
        try:
            _ = self._get(path=path)
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
            >>> path = "https://testazfs.blob.core.windows.net/test_container"
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

    def walk(self, path: str, max_depth=2):
        pass

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
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
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
            >>> pattern_path = "https://testazfs.blob.core.windows.net/test_container/some_folder/*.csv"
            >>> azc.glob(path=pattern_path)
            [
                "https://testazfs.blob.core.windows.net/test_container/some_folder/test1.csv",
                "https://testazfs.blob.core.windows.net/test_container/some_folder/test2.csv",
                "https://testazfs.blob.core.windows.net/test_container/some_folder/test3.csv"
            ]
            glob() can use any path
            >>> pattern_path = "https://testazfs.blob.core.windows.net/test_container/some_folder/test1.*"
            >>> azc.glob(path=pattern_path)
            [
                "https://testazfs.blob.core.windows.net/test_container/some_folder/test1.csv",
                "https://testazfs.blob.core.windows.net/test_container/some_folder/test1.json"
            ]
            also deeper folders
            >>> pattern_path = "https://testazfs.blob.core.windows.net/test_container/some_folder/*/*.csv"
            >>> azc.glob(path=pattern_path)
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

        acceptable_folder_pattern = r"(?P<root_folder>[^\*.]+)/(?P<folders>.*)"
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
        file_list = self._client.get_client(account_kind=account_kind).ls(path=base_path, file_path=root_folder)
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
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            you can read csv file in azure blob storage
            >>> data = azc.get(path=path)
            `download()` is same method as `get()`
            >>> data = azc.download(path=path)

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
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            >>> for l in azc.read_line_iter(path=path)
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
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            >>> chunk_size = 100
            >>> for df in azc.read_csv_chunk(path=path, chunk_size=chunk_size):
            ...   print(df)
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
            >>> import pandas as pd
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            you can read and write csv file in azure blob storage
            >>> df = azc.read_csv(path=path)
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
            >>> import pandas as pd
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.tsv"
            you can read and write csv file in azure blob storage
            >>> df = azc.read_table(path=path)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df = pd.read_table_az(path)

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
            >>> import pandas as pd
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.pkl"
            you can read and write csv file in azure blob storage
            >>> df = azc.read_pickle(path=path)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df = pd.read_pickle_az(path)
            you can use difference compression
            >>> with azc:
            >>>     df = pd.read_pickle_az(path, compression="bz2")

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
            >>> import pandas as pd
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.parquet"
            you can read and write csv file in azure blob storage
            >>> df = azc.read_parquet(path=path)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df = pd.read_parquet_az(path)


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
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            you can write file in azure blob storage
            >>> data = azc.put(path=path)
            `download()` is same method as `get()`
            >>> data = azc.upload(path=path)

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
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.csv"
            you can read and write csv file in azure blob storage
            >>> azc.write_csv(path=path, df=df)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df.to_csv_az(path)
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
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.tsv"
            you can read and write csv file in azure blob storage
            >>> azc.write_table(path=path, df=df)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df.to_table_az(path)
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
            >>> import pandas as pd
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.pkl"
            you can read and write csv file in azure blob storage
            >>> azc.write_pickle(path=path, df=df)
            Using `with` statement, you can use `pandas`-like methods
            >>> with azc:
            >>>     df.to_pickle_az(path)
            you can use difference compression
            >>> with azc:
            >>>     df.to_pickle_az(path, compression="bz2")

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
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.json"
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
            path: Azure Blob path URL format, ex: ``https://testazfs.blob.core.windows.net/test_container/test1.json``
            data: dict to upload
            **kwargs: keywords to put json.loads(), such as ``indent``.

        Returns:
            True if correctly uploaded

        Examples:
            >>> import azfs
            >>> azc = azfs.AzFileClient()
            >>> path = "https://testazfs.blob.core.windows.net/test_container/test1.json"
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
