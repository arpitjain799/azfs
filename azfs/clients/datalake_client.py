from typing import Union
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeFileClient, FileSystemClient, DataLakeServiceClient
from .client_interface import ClientInterface


class AzDataLakeClient(ClientInterface):

    def _get_service_client(
            self,
            account_url: str,
            credential: Union[DefaultAzureCredential, str]) -> DataLakeServiceClient:
        """
        get DataLakeServiceClient

        Args:
            account_url:
            credential:

        Returns:
            DataLakeServiceClient
        """
        return DataLakeServiceClient(account_url=account_url, credential=credential)

    def _get_file_client(
            self,
            account_url: str,
            file_system: str,
            file_path: str) -> DataLakeFileClient:
        """
        get DataLakeFileClient

        Args:
            account_url:
            file_system:
            file_path:

        Returns:
            DataLakeFileClient

        """
        file_client = self._get_service_client_from_url(
            account_url=account_url,
        ).get_file_client(
            file_system=file_system,
            file_path=file_path)
        return file_client

    def _get_container_client(
            self,
            account_url: str,
            file_system: str) -> FileSystemClient:
        """
        get FileSystemClient

        Args:
            account_url:
            file_system:

        Returns:
            FileSystemClient

        """
        file_system_client = self._get_service_client_from_url(
            account_url=account_url
        ).get_file_system_client(
            file_system=file_system
        )
        return file_system_client

    def _ls(self, path: str, file_path: str):
        file_list = \
            [f.name for f in self.get_container_client_from_path(path=path).get_paths(path=file_path, recursive=True)]
        return file_list

    def _get(self, path: str, **kwargs):
        file_bytes = self.get_file_client_from_path(path).download_file().readall()
        return file_bytes

    def _put(self, path: str, data):
        file_client = self.get_file_client_from_path(path=path)
        _ = file_client.create_file()
        _ = file_client.append_data(data=data, offset=0, length=len(data))
        _ = file_client.flush_data(len(data))
        return True

    def _info(self, path: str):
        return self.get_file_client_from_path(path=path).get_file_properties()

    def _rm(self, path: str):
        self.get_file_client_from_path(path=path).delete_file()
        return True
