from azfs.clients.client_interface import ClientInterface
from azfs.utils import import_optional_dependency
from typing import Union
import functools
from azure.identity import DefaultAzureCredential


# import filedatalake optionally
import_queue = functools.partial(
    import_optional_dependency,
    "azure.storage.queue",
    "To handle Azure Queue storage, the package is required.",
    "azure-storage-queue")


class AzQueueClient(ClientInterface):

    def _get_file_client(
            self,
            storage_account_url: str,
            file_system: str,
            file_path: str,
            credential: Union[DefaultAzureCredential, str]):
        # import queue optionally
        _queue = import_queue()
        file_client = _queue.QueueClient(
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
        pass

    def _ls(self, path: str, file_path: str):
        pass

    def _get(self, path: str):
        pass

    def _put(self, path: str, data):
        pass

    def _info(self, path: str):
        pass

    def _rm(self, path: str):
        pass
