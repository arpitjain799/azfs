from azure.identity import DefaultAzureCredential
from azfs.clients.client_interface import ClientInterface
from azure.storage.blob import (
    BlobClient,
    ContainerClient
)
from typing import Union


class AzBlobClient(ClientInterface):

    def _get_file_client(
            self,
            storage_account_url: str,
            file_system: str,
            file_path: str,
            credential: Union[DefaultAzureCredential, str]):
        file_client = BlobClient(
            account_url=storage_account_url,
            container_name=file_system,
            blob_name=file_path,
            credential=credential
        )
        return file_client

    def _get_service_client(self):
        raise NotImplementedError

    def _get_container_client(
            self,
            storage_account_url: str,
            file_system: str,
            credential: Union[DefaultAzureCredential, str]):
        container_client = ContainerClient(
            account_url=storage_account_url,
            container_name=file_system,
            credential=credential)
        return container_client

    def _ls(self, path: str):
        blob_list = [f.name for f in self.get_container_client_from_path(path=path).list_blobs()]
        return blob_list

    def _download_data(self, path: str):
        file_bytes = self.get_file_client_from_path(path=path).download_blob().readall()
        return file_bytes

    def _upload_data(self, path: str, data):
        self.get_file_client_from_path(path=path).upload_blob(data=data, length=len(data))
        return True

    def _get_properties(self, path: str):
        return self.get_file_client_from_path(path=path).get_blob_properties()

    def _rm(self, path: str):
        self.get_file_client_from_path(path=path).delete_blob()
        return True
