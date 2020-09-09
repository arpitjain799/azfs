from abc import abstractmethod
from typing import Union, Optional
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, ContainerClient
from azure.storage.filedatalake import DataLakeFileClient, FileSystemClient
from azure.storage.queue import QueueClient
from azfs.utils import BlobPathDecoder


# define the types
FileClientType = Union[
    BlobClient,
    DataLakeFileClient,
    QueueClient
]
FileSystemClientType = Union[
    ContainerClient,
    FileSystemClient
]


class ClientInterface:
    """
    The class provides Azure Blob, DataLake and Queue Client interface.
    Abstract methods below are implemented in each inherited classes.

    * _get_file_client
    * _get_service_client
    * _get_container_client
    * _ls
    * _get
    * _put
    """

    def __init__(
            self,
            credential: Optional[Union[str, DefaultAzureCredential]],
            connection_string: Optional[str] = None):
        self.credential = credential
        self.connection_string = connection_string

    @abstractmethod
    def _get_service_client_from_credential(
            self,
            account_url: str,
            credential: Union[DefaultAzureCredential, str]):
        """
        get service_client for Blob, DataLake or Queue Service Client with credential.

        Args:
            account_url: account_url ends with ``/``
            credential: DefaultCredential or string

        Returns:

        """
        raise NotImplementedError

    @abstractmethod
    def _get_service_client_from_connection_string(
            self,
            connection_string: str):
        raise NotImplementedError

    def _get_service_client_from_url(self, account_url):
        if self.credential is not None:
            return self._get_service_client_from_credential(account_url=account_url, credential=self.credential)
        elif self.connection_string is not None:
            return self._get_service_client_from_connection_string(connection_string=self.connection_string)

    def get_service_client_from_url(self, account_url):
        return self._get_service_client_from_url(account_url=account_url)

    def get_file_client_from_path(self, path: str) -> FileClientType:
        """
        get file_client from given path

        Args:
            path: Azure path that ``BlobPathDecode()`` can decode

        Returns:
            Union[BlobClient, DataLakeFileClient, QueueClient]
        """
        account_url, account_kind, file_system, file_path = BlobPathDecoder(path).get_with_url()
        return self._get_file_client(
            account_url=account_url,
            file_system=file_system,
            file_path=file_path)

    @abstractmethod
    def _get_file_client(
            self,
            account_url: str,
            file_system: str,
            file_path: str):
        """
        abstract method to be implemented
        get file_client from given path
        :param account_url:
        :param file_system:
        :param file_path:
        :return:
        """
        raise NotImplementedError

    def get_container_client_from_path(self, path: str) -> FileSystemClientType:
        """
        get container_client from given path

        Args:
            path: Azure path that ``BlobPathDecode()`` can decode

        Returns:
            Union[ContainerClient, FileSystemClient]
        """
        account_url, _, file_system, _ = BlobPathDecoder(path).get_with_url()
        return self._get_container_client(
            account_url=account_url,
            file_system=file_system)

    @abstractmethod
    def _get_container_client(
            self,
            account_url: str,
            file_system: str):
        """
        abstract method to be implemented
        :param account_url:
        :param file_system:
        :return:
        """
        raise NotImplementedError

    def ls(self, path: str, file_path: str):
        return self._ls(path=path, file_path=file_path)

    @abstractmethod
    def _ls(self, path: str, file_path: str):
        """
        abstract method to be implemented
        :param path:
        :param file_path:
        :return:
        """
        raise NotImplementedError

    def get(self, path: str, offset: int = None, length: int = None, **kwargs):
        """
        download data from Azure Blob or DataLake.

        Args:
            path:
            offset:
            length:
            **kwargs:

        Returns:

        """
        return self._get(path=path, offset=offset, length=length, **kwargs)

    @abstractmethod
    def _get(self, path: str, offset: int = None, length: int = None, **kwargs):
        """

        Args:
            path:
            offset:
            length:
            **kwargs:

        Returns:

        """
        raise NotImplementedError

    def put(self, path: str, data):
        return self._put(path=path, data=data)

    @abstractmethod
    def _put(self, path: str, data):
        """
        abstract method to be implemented
        :param path:
        :param data:
        :return:
        """
        raise NotImplementedError

    def create(self, path: str):
        return self._create(path=path)

    @abstractmethod
    def _create(self, path: str):
        raise NotImplementedError

    def append(self, path: str, data, offset: int):
        return self._append(path=path, data=data, offset=offset)

    @abstractmethod
    def _append(self, path: str, data, offset: int):
        raise NotImplementedError

    def info(self, path: str):
        return self._info(path=path)

    @abstractmethod
    def _info(self, path: str):
        """
        abstract method to be implemented
        :param path:
        :return:
        """
        raise NotImplementedError

    def rm(self, path: str):
        return self._rm(path=path)

    @abstractmethod
    def _rm(self, path: str):
        """
        abstract method to be implemented
        :param path:
        :return:
        """
        raise NotImplementedError
