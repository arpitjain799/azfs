import math
from typing import Union
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeFileClient, FileSystemClient, DataLakeServiceClient
from .client_interface import ClientInterface


class AzDataLakeClient(ClientInterface):

    def _get_service_client_from_credential(
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

    def _get_service_client_from_connection_string(
            self,
            connection_string: str):
        return DataLakeServiceClient.from_connection_string(conn_str=connection_string)

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
        # uploadするデータの量
        data_length = len(data)
        # 2 ** 23 = 8_388_608 = ~= 10_000_000
        upload_unit = 2 ** 23
        upload_loop = math.ceil(data_length / upload_unit)
        for idx in range(upload_loop):
            length = min(upload_unit, data_length - idx * upload_unit)
            _ = file_client.append_data(data=data, offset=idx * upload_unit, length=length)
            _ = file_client.flush_data(min((idx + 1) * upload_unit, data_length))
        return True

    def _info(self, path: str):
        return self.get_file_client_from_path(path=path).get_file_properties()

    def _rm(self, path: str):
        self.get_file_client_from_path(path=path).delete_file()
        return True
