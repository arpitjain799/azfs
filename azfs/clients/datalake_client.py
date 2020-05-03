from typing import Union
from azure.identity import DefaultAzureCredential
from azfs.clients.client_interface import ClientInterface
import io
import gzip
from azfs.utils import BlobPathDecoder
from azure.storage.filedatalake import (
    DataLakeFileClient
)


class AzDataLakeClient(ClientInterface):

    def _get_file_client(
            self,
            storage_account_url,
            file_system,
            file_path,
            credential):
        self.file_client = DataLakeFileClient(
            storage_account_url,
            file_system,
            file_path,
            credential=credential)
        return self.file_client

    def _get_service_client(self):
        raise NotImplementedError

    def _get_container_client(self):
        raise NotImplementedError

    def _download_data(self, path: str):
        file_bytes = self._get_file_client_from_path(path).download_file().readall()
        return file_bytes

    def _upload_data(self, path: str, data):
        file_client = self._get_file_client_from_path(path=path)
        _ = file_client.create_file()
        _ = file_client.append_data(data=data, offset=0, length=len(data))
        _ = file_client.flush_data(len(data))
        return True
