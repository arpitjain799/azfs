from typing import Union
from azure.identity import DefaultAzureCredential
import io
import gzip
from azfs.utils import BlobPathDecoder
from azure.storage.blob import (
    BlobClient,
    ContainerClient
)
from azure.storage.filedatalake import (
    DataLakeFileClient,
    FileSystemClient
)


class ClientInterface:
    """
    The class provides Azure Blob and Datalake Container and File Client interface.
    Abstract methods below are implemented in each class
        * _get_file_client
        * _get_service_client
        * _get_container_client
        * _ls
        * _download_data
        * _upload_data
    """

    def __init__(
            self,
            credential: Union[str, DefaultAzureCredential]):
        self.credential = credential

    def get_file_client_from_path(self, path) -> Union[BlobClient, DataLakeFileClient]:
        """
        get file_client from given path
        :param path:
        :return:
        """
        storage_account_url, account_kind, file_system, file_path = BlobPathDecoder(path).get_with_url()
        return self._get_file_client(
            storage_account_url=storage_account_url,
            file_system=file_system,
            file_path=file_path,
            credential=self.credential)

    def _get_file_client(
            self,
            storage_account_url: str,
            file_system: str,
            file_path: str,
            credential: Union[DefaultAzureCredential, str]):
        """
        abstract method to be implemented
        get file_client from given path
        :param storage_account_url:
        :param file_system:
        :param file_path:
        :param credential:
        :return:
        """
        raise NotImplementedError

    def _get_service_client(self):
        """
        abstract method to be implemented
        :return:
        """
        raise NotImplementedError

    def get_container_client_from_path(self, path) -> Union[ContainerClient, FileSystemClient]:
        """
        get container_client from given path
        :param path:
        :return:
        """
        storage_account_url, _, file_system, _ = BlobPathDecoder(path).get_with_url()
        return self._get_container_client(
            storage_account_url=storage_account_url,
            file_system=file_system,
            credential=self.credential)

    def _get_container_client(
            self,
            storage_account_url: str,
            file_system: str,
            credential: Union[DefaultAzureCredential, str]):
        """
        abstract method to be implemented
        :param storage_account_url:
        :param file_system:
        :param credential:
        :return:
        """
        raise NotImplementedError

    def ls(self, path: str):
        return self._ls(path=path)

    def _ls(self, path: str):
        """
        abstract method to be implemented
        :param path:
        :return:
        """
        raise NotImplementedError

    def download_data(self, path: str):
        """
        download data from Azure Blob or DataLake.
        :param path:
        :return:
        """
        file_bytes = self._download_data(path=path)

        # gzip圧縮ファイルは一旦ここで展開
        if path.endswith(".gz"):
            file_bytes = gzip.decompress(file_bytes)

        if type(file_bytes) is bytes:
            file_to_read = io.BytesIO(file_bytes)
        else:
            file_to_read = file_bytes
        return file_to_read

    def _download_data(self, path: str):
        """
        abstract method to be implemented
        :param path:
        :return:
        """
        raise NotImplementedError

    def upload_data(self, path: str, data):
        return self._upload_data(path=path, data=data)

    def _upload_data(self, path: str, data):
        """
        abstract method to be implemented
        :param path:
        :param data:
        :return:
        """
        raise NotImplementedError

    def get_properties(self, path: str):
        return self._get_properties(path=path)

    def _get_properties(self, path: str):
        """
        abstract method to be implemented
        :param path:
        :return:
        """
        raise NotImplementedError

    def rm(self, path: str):
        return self._rm(path=path)

    def _rm(self, path: str):
        """
        abstract method to be implemented
        :param path:
        :return:
        """
        raise NotImplementedError
