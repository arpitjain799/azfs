import azfs
from azfs.clients.datalake_client import AzDataLakeClient
from azure.storage.filedatalake import FileSystemClient, DataLakeFileClient

credential = ""
azc = azfs.AzFileClient()
datalake_client = AzDataLakeClient(credential=credential)
test_file_path = "https://test.dfs.core.windows.net/test/test.csv"
test_file_ls_path = "https://test.dfs.core.windows.net/test/"


class BlobMock:
    # dummy blob file class
    def __init__(self, name):
        self.name = name


def test_blob_info(mocker):
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


def test_blob_info_directory(mocker):
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


def test_blob_upload(mocker):
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
    result = datalake_client.put(path=test_file_path, data={})

    assert result
    append_data_mock.assert_called_with(data={}, offset=0, length=len({}))
    flush_data_mock.assert_called_with(len({}))


def test_blob_rm(mocker):
    # ======================== #
    # test for datalake_client #
    # ======================== #

    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = True

    mocker.patch.object(DataLakeFileClient, "delete_file", func_mock)
    result = datalake_client.rm(path=test_file_path)
    assert result


def test_blob_ls(mocker):
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
