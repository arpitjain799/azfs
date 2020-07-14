import pytest
from azfs.clients.blob_client import AzBlobClient
from azure.storage.blob import BlobClient, ContainerClient
import azfs

credential = ""
connection_string = "DefaultEndpointsProtocol=https;AccountName=xxxx;AccountKey=xxxx;EndpointSuffix=core.windows.net"
azc = azfs.AzFileClient()
blob_client_credential = AzBlobClient(credential=credential)
blob_client_connection_string = AzBlobClient(credential=None, connection_string=connection_string)
test_file_path = "https://test.blob.core.windows.net/test/test.csv"
test_file_ls_path = "https://test.blob.core.windows.net/test/"


class BlobMock:
    # dummy blob file class
    def __init__(self, name):
        self.name = name


@pytest.mark.parametrize("blob_client", [
    # blob client
    blob_client_credential,
    blob_client_connection_string,
])
def test_blob_info(mocker, blob_client):
    # ===================== #
    # test for AzBlobClient #
    # ===================== #

    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = {
            "name": "test.csv",
            "size": 500,
            "creation_time": "creation_time",
            "last_modified": "last_modified",
            "etag": "etag",
            "content_settings": {
                "content_type": "content"
            }
        }

    mocker.patch.object(BlobClient, "get_blob_properties", func_mock)
    info = blob_client.info(path=test_file_path)
    assert "name" in info

    # ===================== #
    # test for AzFileClient #
    # ===================== #
    info = azc.info(path=test_file_path)
    assert "name" in info
    size = azc.size(path=test_file_path)
    assert size == 500
    check_sum = azc.checksum(path=test_file_path)
    assert check_sum == "etag"
    result = azc.isfile(path=test_file_path)
    assert result


@pytest.mark.parametrize("blob_client", [
    # blob client
    blob_client_credential,
    blob_client_connection_string,
])
def test_blob_info_error(mocker, blob_client):
    func_mock = mocker.MagicMock()
    func_mock.side_effect = IOError
    mocker.patch.object(BlobClient, "get_blob_properties", func_mock)

    result = azc.isfile(path=test_file_path)
    assert not result
    result = azc.isdir(path=test_file_path)
    assert not result


@pytest.mark.parametrize("blob_client", [
    # blob client
    blob_client_credential,
    blob_client_connection_string,
])
def test_blob_upload(mocker, blob_client):
    # ===================== #
    # test for AzBlobClient #
    # ===================== #

    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = True

    mocker.patch.object(BlobClient, "upload_blob", func_mock)
    result = blob_client.put(path=test_file_path, data={})
    assert result


@pytest.mark.parametrize("blob_client", [
    # blob client
    blob_client_credential,
    blob_client_connection_string,
])
def test_blob_rm(mocker, blob_client):
    # ===================== #
    # test for AzBlobClient #
    # ===================== #

    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = True

    mocker.patch.object(BlobClient, "delete_blob", func_mock)
    result = blob_client.rm(path=test_file_path)
    assert result


@pytest.mark.parametrize("blob_client", [
    # blob client
    blob_client_credential,
    blob_client_connection_string,
])
def test_blob_ls(mocker, blob_client):
    # ===================== #
    # test for AzBlobClient #
    # ===================== #

    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = [
        BlobMock("test.csv"),
    ]

    mocker.patch.object(ContainerClient, "list_blobs", func_mock)
    file_list = blob_client.ls(path=test_file_ls_path, file_path=test_file_ls_path)
    assert file_list
