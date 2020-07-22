import base64
from typing import Union
from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueClient, QueueServiceClient
from .client_interface import ClientInterface


class AzQueueClient(ClientInterface):

    def _get_service_client_from_credential(
            self,
            account_url: str,
            credential: Union[DefaultAzureCredential, str]):
        """
        get QueueServiceClient

        Args:
            account_url:
            credential:

        Returns:

        """
        return QueueServiceClient(account_url=account_url, credential=credential)

    def _get_service_client_from_connection_string(
            self,
            connection_string: str):
        return QueueServiceClient.from_connection_string(conn_str=connection_string)

    def _get_file_client(
            self,
            account_url: str,
            file_system: str,
            file_path: str) -> QueueClient:
        """
        get QueueClient

        Args:
            account_url:
            file_system:
            file_path:

        Returns:

        """
        queue_client = self._get_service_client_from_url(
            account_url=account_url
        ).get_queue_client(
            queue=file_system
        )
        return queue_client

    def _get_container_client(
            self,
            account_url: str,
            file_system: str):
        """
        no correspond method to _container_client() in QueueClient

        Args:
            account_url:
            file_system:

        Returns:

        """

        raise NotImplementedError

    def _ls(self, path: str, file_path: str):
        return self.get_file_client_from_path(path).peek_messages(16)

    def _get(self, path: str, offset: int = None, length: int = None, **kwargs):
        """

        Args:
            path:
            offset:
            length:
            **kwargs: ``delete`` or ``delete_after_receive`` are acceptable, and it means after you get message
                from queue, the message you receive will be deleted. By default, the message will not deleted.

        Returns:

        """
        delete_after_receive = True
        if "delete" in kwargs:
            delete_after_receive = kwargs['delete']
        elif "delete_after_receive" in kwargs:
            delete_after_receive = kwargs['delete_after_receive']

        queue_client = self.get_file_client_from_path(path)
        # get queue iterator
        message_itr = queue_client.receive_messages()
        try:
            received_message = next(message_itr)
            message_id = received_message['id']
            pop_receipt = received_message['pop_receipt']
            # decode with base64
            received_message['content'] = base64.b64decode(received_message['content'].encode('utf-8')).decode('utf-8')

            # delete message in queue
            if delete_after_receive:
                queue_client.delete_message(message_id, pop_receipt=pop_receipt)
            return received_message
        except StopIteration:
            return {"status": "error", "message": "queue not found"}

    def _put(self, path: str, data):
        """
        put message in queue with base64-encoded.

        Args:
            path:
            data:

        Returns:

        See Also:
            https://azuresdkdocs.blob.core.windows.net/$web/python/azure-storage-queue/12.1.0/_modules/azure/storage/queue/_message_encoding.html

        """
        # encode with base64
        encoded_data = base64.b64encode(data.encode('utf-8')).decode('utf-8')
        put_data = self.get_file_client_from_path(path).send_message(encoded_data)
        put_data['content'] = base64.b64decode(put_data['content'].encode('utf-8')).decode('utf-8')
        return put_data

    def _create(self, path: str):
        raise NotImplementedError

    def _append(self, path: str, data, offset: int):
        raise NotImplementedError

    def _info(self, path: str):
        """
        no correspond method to _info() in QueueClient

        Args:
            path:

        Returns:

        """
        raise NotImplementedError

    def _rm(self, path: str):
        """
        no correspond method to _rm() in QueueClient

        Args:
            path:

        Returns:

        """
        raise NotImplementedError
