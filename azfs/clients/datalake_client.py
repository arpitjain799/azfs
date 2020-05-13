from azfs.clients.client_interface import ClientInterface
from azfs.utils import import_optional_dependency
from typing import Union
import functools
from azure.identity import DefaultAzureCredential


# import filedatalake optionally
import_filedatalake = functools.partial(
    import_optional_dependency,
    "azure.storage.filedatalake",
    "To handle Azure Datalake storage, the package is required.",
    "azure-storage-file-datalake")


class AzDataLakeClient(ClientInterface):

    def _get_file_client(
            self,
            storage_account_url: str,
            file_system: str,
            file_path: str,
            credential: Union[DefaultAzureCredential, str]):
        filedatalake = import_filedatalake()
        file_client = filedatalake.DataLakeFileClient(
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
        filedatalake = import_filedatalake()
        file_system = filedatalake.FileSystemClient(
            account_url=storage_account_url,
            file_system_name=file_system,
            credential=credential)
        return file_system

    def _ls(self, path: str, file_path: str):
        file_list = \
            [f.name for f in self.get_container_client_from_path(path=path).get_paths(path=file_path, recursive=True)]
        return file_list

    def _get(self, path: str):
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
