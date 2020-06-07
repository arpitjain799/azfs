from azfs.clients.client_interface import ClientInterface
from typing import Union
from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueClient
import base64


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
        raise NotImplementedError

    def _ls(self, path: str, file_path: str):
        return self.get_file_client_from_path(path).peek_messages(16)

    def _get(self, path: str, **kwargs):
        """

        :param path:
        :param kwargs:
        :return:
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
        # encode with base64
        encoded_data = base64.b64encode(data.encode('utf-8')).decode('utf-8')
        put_data = self.get_file_client_from_path(path).send_message(encoded_data)
        put_data['content'] = base64.b64decode(put_data['content'].encode('utf-8')).decode('utf-8')
        return put_data

    def _info(self, path: str):
        """
        no correspond method to _info()
        :param path:
        :return:
        """
        raise NotImplementedError

    def _rm(self, path: str):
        """
        no correspond method to _rm()
        :param path:
        :return:
        """
        raise NotImplementedError
