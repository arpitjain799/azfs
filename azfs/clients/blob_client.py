from azfs.clients.client_interface import ClientInterface
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    ContainerClient
)
from azfs.utils import BlobPathDecoder


class AzBlobClient(ClientInterface):

    def _get_file_client(
            self,
            storage_account_url,
            file_system,
            file_path,
            credential):
        if self.file_client is None:
            self.file_client = BlobClient(
                account_url=storage_account_url,
                container_name=file_system,
                blob_name=file_path,
                credential=credential
            )
        return self.file_client

    def _get_service_client(self):
        raise NotImplementedError

    def _get_container_client(self):
        raise NotImplementedError

    def _download_data(self, path: str):
        storage_account_url, account_kind, file_system, file_path = BlobPathDecoder(path).get_with_url()
        if self.file_client is None:
            self.file_client = self._get_file_client(
                storage_account_url=storage_account_url,
                file_system=file_system,
                file_path=file_path,
                credential=self.credential)
        file_bytes = self.file_client.download_blob().readall()
        return file_bytes

    def _upload_data(self):
        raise NotImplementedError
