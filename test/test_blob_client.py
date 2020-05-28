from azfs.clients.blob_client import AzBlobClient
from azure.storage.blob import BlobClient, ContainerClient

credential = ""
blob_client = AzBlobClient(credential=credential)
test_file_path = "https://test.blob.core.windows.net/test/test.csv"
test_file_ls_path = "https://test.blob.core.windows.net/test/"


class BlobMock:
    # dummy blob file class
    def __init__(self, name):
        self.name = name


def test_blob_info(mocker):
    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = {
            "name": "test.csv",
            "size": 500,
            "creation_time": "creation_time",
            "last_modified": "last_modified",
            "etag": "etag",
            "content_type": "content_type",
            "type": "file"
        }

    mocker.patch.object(BlobClient, "get_blob_properties", func_mock)
    info = blob_client.info(path=test_file_path)
    assert "name" in info


def test_blob_upload(mocker):
    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = True

    mocker.patch.object(BlobClient, "upload_blob", func_mock)
    result = blob_client.put(path=test_file_path, data={})
    assert result


def test_blob_rm(mocker):
    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = True

    mocker.patch.object(BlobClient, "delete_blob", func_mock)
    result = blob_client.rm(path=test_file_path)
    assert result


def test_blob_ls(mocker):
    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = [
        BlobMock("test.csv"),
    ]

    mocker.patch.object(ContainerClient, "list_blobs", func_mock)
    file_list = blob_client.ls(path=test_file_ls_path, file_path=test_file_ls_path)
    assert file_list
