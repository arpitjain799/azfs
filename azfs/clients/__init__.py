from typing import Union
from .blob_client import AzBlobClient
from .datalake_client import AzDataLakeClient
from .queue_client import AzQueueClient


class MetaClient(type):
    """
    A metaclass which have AzBlobClient or AzDataLakeClient in class dictionary.
    if another storage type is added, add new storage type as {"***": Class<Az***Client>}
    """
    def __new__(mcs, name, bases, dictionary):
        cls = type.__new__(mcs, name, bases, dictionary)
        # set Clients
        clients = {
            'dfs': AzDataLakeClient,
            'blob': AzBlobClient,
            'queue': AzQueueClient
        }
        cls.CLIENTS = clients
        return cls


class AbstractClient(metaclass=MetaClient):
    pass


class AzfsClient(AbstractClient):
    """
    Abstract Client for AzBlobClient, AzDataLakeClient and AzQueueClient.

    Examples:
        >>> blob_client = AzfsClient(credential="...").get_client("blob")
        # or
        >>> datalake_client = AzfsClient(credential="...").get_client("dfs")
        # AzfsClient provide easy way to access functions implemented in AzBlobClient and AzDataLakeClient, as below
        >>> data_path = "https://testazfs.blob.core.windows.net/test_container/test1.json"
        >>> data = AzfsClient(credential="...").get_client("blob").get(path=data_path)

    """
    CLIENTS = {}

    def __init__(self, credential, connection_string):
        self._credential = credential
        self._connection_string = connection_string

    def get_client(self, account_kind: str) -> Union[AzBlobClient, AzDataLakeClient, AzQueueClient]:
        """
        get AzBlobClient, AzDataLakeClient or AzQueueClient depending on account_kind

        Args:
            account_kind: blob, dfs or queue

        Returns:
            Union[AzBlobClient, AzDataLakeClient, AzQueueClient]

        Examples:
            >>> azfs_client = AzfsClient(credential="...")
            >>> AzBlobClient = azfs_client.get_client("blob")
        """
        return self.CLIENTS[account_kind](credential=self._credential, connection_string=self._connection_string)


class TextReader:
    """
    The class is to provide line-based reading iterator.
    Reading file should be ends-with "\n", otherwise last line will be ignored.
    """
    def __init__(self, client, path: str, offset: int = 0, length: int = 2 ** 14, size: int = None):
        self._client = client
        self._size = client.info(path).get("size", size)
        self._rest_part = b""
        self._offset = offset
        self._length = length
        self._read_length = length
        self._iter = 0
        self._path = path
        self._byte_text = None
        self._current_chunk_lines = None
        self._current_chunk_lines_length = None
        self._line_counter = 0

    def get_chunk(self) -> bytes:
        """
        get bytes-data from AzureStorage

        Returns:
            AzureStorageFile-byte [start: end]
        """
        if self._read_length < 0:
            raise StopIteration()
        rtn = self._rest_part + self._client.get(path=self._path, offset=self._offset, length=self._length)
        self._iter += 1
        self._offset = self._iter * self._length
        self._read_length = min(self._length, self._size - self._offset)
        return rtn

    def next_line(self) -> bytes:
        if self._byte_text is None or self._line_counter >= self._current_chunk_lines_length:
            self._byte_text = self.get_chunk()
            chunk_lines = self._byte_text.split("\n".encode('utf-8'))
            self._current_chunk_lines = chunk_lines[:-1]
            self._current_chunk_lines_length = len(self._current_chunk_lines)
            self._line_counter = 0
            self._rest_part = chunk_lines[-1]
        current_line = self._current_chunk_lines[self._line_counter]
        self._line_counter += 1
        return current_line

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        try:
            return self.next_line()
        except StopIteration:
            raise
