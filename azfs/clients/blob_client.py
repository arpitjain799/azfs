from azfs.clients.client_interface import ClientInterface
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    ContainerClient
)


class AzBlobClient(ClientInterface):

    def _get_file_client(
            self,
            storage_account_url,
            file_system,
            file_path,
            credential):
        self.file_client = BlobClient(
            account_url=storage_account_url,
            container_name=file_system,
            blob_name=file_path,
            credential=credential
        )
        return self.file_client

    def _get_service_client(self):
        raise NotImplementedError

    def _get_container_client(
            self,
            storage_account_url: str,
            file_system: str,
            credential):
        container_client = ContainerClient(
            account_url=storage_account_url,
            container_name=file_system,
            credential=credential)
        return container_client

    def _ls(self, path: str):
        blob_list = [f.name for f in self.get_container_client_from_path(path=path).list_blobs()]
        return blob_list

    def _download_data(self, path: str):
        file_bytes = self._get_file_client_from_path(path=path).download_blob().readall()
        return file_bytes

    def _upload_data(self, path: str, data):
        self._get_file_client_from_path(path=path).upload_blob(data=data, length=len(data))
        return True
