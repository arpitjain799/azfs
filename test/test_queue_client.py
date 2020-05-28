import pytest
from azfs.clients.queue_client import AzQueueClient


credential = ""
queue_client = AzQueueClient(credential=credential)
test_queue_path = "https://test.queue.core.windows.net/test_queue/"


def test_not_implemented_error():
    with pytest.raises(NotImplementedError):
        queue_client.info(path=test_queue_path)

    with pytest.raises(NotImplementedError):
        queue_client.rm(path=test_queue_path)

    with pytest.raises(NotImplementedError):
        queue_client.get_container_client_from_path(path=test_queue_path)
