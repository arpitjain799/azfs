from azfs.clients.client_interface import ClientInterface
from azure.storage.filedatalake import (
    DataLakeFileClient,
    FileSystemClient
)
from typing import Union
from azure.identity import DefaultAzureCredential


class AzDataLakeClient(ClientInterface):

    def _get_file_client(
            self,
            storage_account_url: str,
            file_system: str,
            file_path: str,
            credential: Union[DefaultAzureCredential, str]):
        file_client = DataLakeFileClient(
            storage_account_url,
            file_system,
            file_path,
            credential=credential)
        return file_client

    def _get_service_client(self):
        raise NotImplementedError

    def _get_container_client(
            self,
            storage_account_url: str,
            file_system: str,
            credential: Union[DefaultAzureCredential, str]):
        file_system = FileSystemClient(
            account_url=storage_account_url,
            file_system_name=file_system,
            credential=credential)
        return file_system

    def _ls(self, path: str):
        file_list = [f.name for f in self.get_container_client_from_path(path=path).get_paths()]
        return file_list

    def _download_data(self, path: str):
        file_bytes = self.get_file_client_from_path(path).download_file().readall()
        return file_bytes

    def _upload_data(self, path: str, data):
        file_client = self.get_file_client_from_path(path=path)
        _ = file_client.create_file()
        _ = file_client.append_data(data=data, offset=0, length=len(data))
        _ = file_client.flush_data(len(data))
        return True

    def _get_properties(self, path: str):
        return self.get_file_client_from_path(path=path).get_file_properties()

    def _rm(self, path: str):
        self.get_file_client_from_path(path=path).delete_file()
        return True
