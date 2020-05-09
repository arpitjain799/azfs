from azfs.clients.blob_client import AzBlobClient
from azfs.clients.datalake_client import AzDataLakeClient
from typing import Union


class MetaClient(type):

    def __new__(mcs, name, bases, dictionary):
        cls = type.__new__(mcs, name, bases, dictionary)
        skills = {'dfs': AzDataLakeClient,
                  'blob': AzBlobClient}
        cls.CLIENTS = skills
        return cls


class AbstractClient(metaclass=MetaClient):
    pass


class AzfsClient(AbstractClient):
    CLIENTS = {}

    @classmethod
    def get(cls, skill_key, credential) -> Union[AzBlobClient, AzDataLakeClient]:
        return cls.CLIENTS[skill_key](credential=credential)
