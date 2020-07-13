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

