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
    Interface of AzBlobClient, AzDataLakeClient and AzQueueClient.
    Different instances can be obtained as below

    Examples:
        >>> blob_client = AzfsClient.get("blob", "***")
        # or
        >>> datalake_client = AzfsClient.get("dfs", "***")
        # AzfsClient provide easy way to access functions implemented in AzBlobClient and AzDataLakeClient, as below
        # path is azure storage url
        data = AzfsClient.get(account_kind="blob", credential="...").download_data(path)

    """
    CLIENTS = {}

    @classmethod
    def get(cls, account_kind: str, credential) -> Union[AzBlobClient, AzDataLakeClient]:
        """
        get AzBlobClient, AzDataLakeClient or AzQueueClient depending on account_kind

        Args:
            account_kind: blob, dfs or queue
            credential: AzureDefaultCredential or string

        Returns:
            Union[AzBlobClient, AzDataLakeClient, AzQueueClient]

        Examples:
            >>> AzBlobClient = AzfsClient(account_kind="blob", credential="...")
        """
        return cls.CLIENTS[account_kind](credential=credential)
