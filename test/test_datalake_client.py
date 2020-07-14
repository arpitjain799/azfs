import json
import pytest
from azfs.clients.datalake_client import AzDataLakeClient
from azure.storage.filedatalake import FileSystemClient, DataLakeFileClient
import azfs

credential = ""
connection_string = "DefaultEndpointsProtocol=https;AccountName=xxxx;AccountKey=xxxx;EndpointSuffix=core.windows.net"
azc = azfs.AzFileClient()
datalake_client_credential = AzDataLakeClient(credential=credential)
datalake_client_connection_string = AzDataLakeClient(credential=None, connection_string=connection_string)
test_file_path = "https://test.dfs.core.windows.net/test/test.csv"
test_file_ls_path = "https://test.dfs.core.windows.net/test/"


class BlobMock:
    # dummy blob file class
    def __init__(self, name):
        self.name = name


@pytest.mark.parametrize("datalake_client", [
    # blob client
    datalake_client_credential,
    datalake_client_connection_string,
])
def test_blob_info(mocker, datalake_client):
    # ======================== #
    # test for datalake_client #
    # ======================== #

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

    mocker.patch.object(DataLakeFileClient, "get_file_properties", func_mock)
    info = datalake_client.info(path=test_file_path)
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
    result = azc.isdir(path=test_file_path)
    assert not result


def test_blob_info_error(mocker):
    func_mock = mocker.MagicMock()
    func_mock.side_effect = IOError
    mocker.patch.object(DataLakeFileClient, "get_file_properties", func_mock)

    result = azc.isfile(path=test_file_path)
    assert not result
    result = azc.isdir(path=test_file_path)
    assert not result


@pytest.mark.parametrize("datalake_client", [
    # blob client
    datalake_client_credential,
    datalake_client_connection_string,
])
def test_blob_info_directory(mocker, datalake_client):
    # ======================== #
    # test for datalake_client #
    # ======================== #

    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = {
            "name": "test/",
            "size": 0,
            "creation_time": "creation_time",
            "last_modified": "last_modified",
            "etag": "etag",
            "metadata": {
                "hdi_isfolder": "hdi_isfolder"
            }
        }

    mocker.patch.object(DataLakeFileClient, "get_file_properties", func_mock)
    info = datalake_client.info(path=test_file_path)
    assert "name" in info

    # ===================== #
    # test for AzFileClient #
    # ===================== #
    info = azc.info(path=test_file_path)
    assert "name" in info
    size = azc.size(path=test_file_path)
    assert size == 0
    check_sum = azc.checksum(path=test_file_path)
    assert check_sum == "etag"
    result = azc.isfile(path=test_file_path)
    assert not result
    result = azc.isdir(path=test_file_path)
    assert result


@pytest.mark.parametrize("datalake_client", [
    # blob client
    datalake_client_credential,
    datalake_client_connection_string,
])
def test_blob_upload(mocker, datalake_client):
    # ======================== #
    # test for datalake_client #
    # ======================== #

    # mock
    create_file_mock = mocker.MagicMock()
    create_file_mock.return_value = True
    append_data_mock = mocker.MagicMock()
    append_data_mock.return_value = True
    flush_data_mock = mocker.MagicMock()
    flush_data_mock.return_value = True

    mocker.patch.object(DataLakeFileClient, "create_file", create_file_mock)
    mocker.patch.object(DataLakeFileClient, "append_data", append_data_mock)
    mocker.patch.object(DataLakeFileClient, "flush_data", flush_data_mock)
    data = json.dumps({"example": "data"})
    result = datalake_client.put(path=test_file_path, data=data)

    assert result
    append_data_mock.assert_called_with(data=data, offset=0, length=len(data))
    flush_data_mock.assert_called_with(len(data))


@pytest.mark.parametrize("datalake_client", [
    # blob client
    datalake_client_credential,
    datalake_client_connection_string,
])
def test_blob_rm(mocker, datalake_client):
    # ======================== #
    # test for datalake_client #
    # ======================== #

    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = True

    mocker.patch.object(DataLakeFileClient, "delete_file", func_mock)
    result = datalake_client.rm(path=test_file_path)
    assert result


@pytest.mark.parametrize("datalake_client", [
    # blob client
    datalake_client_credential,
    datalake_client_connection_string,
])
def test_blob_ls(mocker, datalake_client):
    # ======================== #
    # test for datalake_client #
    # ======================== #

    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = [
        BlobMock("test.csv"),
    ]

    mocker.patch.object(FileSystemClient, "get_paths", func_mock)
    file_list = datalake_client.ls(path=test_file_ls_path, file_path=test_file_ls_path)
    assert file_list
