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

    def _get(self, path: str, offset: int = None, length: int = None, **kwargs):
        file_bytes = self.get_file_client_from_path(path).download_file(offset=offset, length=length).readall()
        return file_bytes

    def _put(self, path: str, data):
        """
        In DataLake Storage Account, uploading the file over 100MB may raise Exception like
        `(RequestBodyTooLarge) The request body is too large and exceeds the maximum permissible limit`.

        So in order to avoid the exception above, data are uploaded by appending.

        Args:
            path:
            data:

        Returns:

        """
        file_client = self.get_file_client_from_path(path=path)
        _ = file_client.create_file()
        # upload data
        data_length = len(data)
        # 2 ** 23 = 8_388_608 ~= 10_000_000
        upload_unit = 2 ** 23
        append_times = math.ceil(data_length / upload_unit)
        # to avoid uploading limitation in one time
        for idx in range(append_times):
            start = idx * upload_unit
            end = min((idx + 1) * upload_unit, data_length)
            split_data = data[start:end]
            # upload data
            self.append(path=path, data=split_data, offset=start)
        return True

    def _create(self, path: str) -> dict:
        file_client = self.get_file_client_from_path(path=path)
        response = file_client.create_file()
        return response

    def _append(self, path: str, data, offset: int) -> dict:
        file_client = self.get_file_client_from_path(path=path)
        data_length = len(data)
        response = file_client.append_data(data=data, offset=offset, length=data_length)
        # write data
        _ = file_client.flush_data(offset + data_length)
        return response

    def _info(self, path: str):
        return self.get_file_client_from_path(path=path).get_file_properties()

    def _rm(self, path: str):
        self.get_file_client_from_path(path=path).delete_file()
        return True
