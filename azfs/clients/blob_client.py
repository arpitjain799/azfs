from typing import Union
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, ContainerClient, BlobServiceClient
from .client_interface import ClientInterface


class AzBlobClient(ClientInterface):

    def _get_service_client_from_credential(
            self,
            account_url: str,
            credential: Union[DefaultAzureCredential, str]) -> BlobServiceClient:
        """
        get BlobServiceClient

        Args:
            account_url:
            credential:

        Returns:
            BlobServiceClient
        """
        return BlobServiceClient(account_url=account_url, credential=credential)

    def _get_service_client_from_connection_string(
            self,
            connection_string: str):
        return BlobServiceClient.from_connection_string(conn_str=connection_string)

    def _get_file_client(
            self,
            account_url: str,
            file_system: str,
            file_path: str) -> BlobClient:
        """
        get BlobClient

        Args:
            account_url:
            file_system:
            file_path:

        Returns:
            BlobClient
        """
        file_client = self._get_service_client_from_url(
            account_url=account_url,
        ).get_blob_client(
            container=file_system,
            blob=file_path
        )
        return file_client

    def _get_container_client(
            self,
            account_url: str,
            file_system: str) -> ContainerClient:
        """
        get ContainerClient

        Args:
            account_url:
            file_system:

        Returns:
            ContainerClient
        """
        container_client = self._get_service_client_from_url(
            account_url=account_url,
        ).get_container_client(
            container=file_system
        )
        return container_client

    def _ls(self, path: str, file_path: str):
        blob_list = \
            [f.name for f in self.get_container_client_from_path(path=path).list_blobs(name_starts_with=file_path)]
        return blob_list

    def _get(self, path: str, offset: int = None, length: int = None, **kwargs):
        file_bytes = self.get_file_client_from_path(path=path).download_blob(offset=offset, length=length).readall()
        return file_bytes

    def _put(self, path: str, data):
        self.get_file_client_from_path(path=path).upload_blob(
            data=data,
            length=len(data),
            overwrite=True
        )
        return True

    def _create(self, path: str):
        raise NotImplementedError

    def _append(self, path: str, data, offset: int):
        raise NotImplementedError

    def _info(self, path: str):
        return self.get_file_client_from_path(path=path).get_blob_properties()

    def _rm(self, path: str):
        self.get_file_client_from_path(path=path).delete_blob()
        return True
