from azfs.clients.blob_client import AzBlobClient
from azfs.clients.datalake_client import AzDataLakeClient
from typing import Union


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
            'blob': AzBlobClient
        }
        cls.CLIENTS = clients
        return cls


class AbstractClient(metaclass=MetaClient):
    pass


class AzfsClient(AbstractClient):
    """
    Interface of AzBlobClient and AzDataLakeClient.
    Different instances can be obtained as below

    blob_client = AzfsClient.get("blob", "***")
    or
    datalake_client = AzfsClient.get("dfs", "***")

    AzfsClient provide easy way to access functions implemented in AzBlobClient and AzDataLakeClient, as below

    # path is azure storage url
    data = AzfsClient.get("blob", "***").download_data(path)

    """
    CLIENTS = {}

    @classmethod
    def get(cls, account_kind: str, credential) -> Union[AzBlobClient, AzDataLakeClient]:
        """
        get AzBlobClient or AzDataLakeClient depending on account_kind
        :param account_kind: currently blob or dfs
        :param credential: AzureDefaultCredential or string
        :return:
        """
        return cls.CLIENTS[account_kind](credential=credential)
