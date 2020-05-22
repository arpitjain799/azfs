from azfs.clients.client_interface import ClientInterface
from typing import Union
from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueClient


class AzQueueClient(ClientInterface):

    def _get_file_client(
            self,
            storage_account_url: str,
            file_system: str,
            file_path: str,
            credential: Union[DefaultAzureCredential, str]) -> QueueClient:
        queue_client = QueueClient(
            account_url=storage_account_url,
            queue_name=file_system,
            credential=credential)
        return queue_client

    def _get_service_client(self):
        raise NotImplementedError

    def _get_container_client(
            self,
            storage_account_url: str,
            file_system: str,
            credential: Union[DefaultAzureCredential, str]):
        pass

    def _ls(self, path: str, file_path: str):
        return self.get_file_client_from_path(path).peek_messages(16)

    def _get(self, path: str):
        pass

    def _put(self, path: str, data):
        return self.get_file_client_from_path(path).send_message(data)

    def _info(self, path: str):
        pass

    def _rm(self, path: str):
        pass
