from typing import Union
from azure.identity import DefaultAzureCredential
import io
import gzip
from azfs.utils import BlobPathDecoder


class ClientInterface:

    def __init__(
            self,
            credential: Union[str, DefaultAzureCredential]):
        self.credential = credential
        self.service_client = None
        self.file_client = None
        self.container_client = None

    def _get_file_client_from_path(self, path):
        storage_account_url, account_kind, file_system, file_path = BlobPathDecoder(path).get_with_url()
        if self.file_client is None:
            self.file_client = self._get_file_client(
                storage_account_url=storage_account_url,
                file_system=file_system,
                file_path=file_path,
                credential=self.credential)
        return self.file_client

    def _get_file_client(
            self,
            storage_account_url,
            file_system,
            file_path,
            credential):
        raise NotImplementedError

    def _get_service_client(self):
        raise NotImplementedError

    def _get_container_client(self):
        raise NotImplementedError

    def download_data(self, path: str):
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
        raise NotImplementedError

    def upload_data(self, path: str, data):
        return self._upload_data(path=path, data=data)

    def _upload_data(self, path: str, data):
        raise NotImplementedError
