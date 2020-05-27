import pytest
from azfs.clients.queue_client import AzQueueClient


def test_not_implemented_error():
    credential = ""
    path = "https://test.queue.core.windows.net/"
    queue_client = AzQueueClient(credential=credential)
    with pytest.raises(NotImplementedError):
        queue_client.info(path=path)

    with pytest.raises(NotImplementedError):
        queue_client.rm(path=path)

    with pytest.raises(NotImplementedError):
        queue_client.get_container_client_from_path(path=path)
