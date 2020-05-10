import pytest
from azfs.clients.blob_client import AzBlobClient
from azfs.clients.datalake_client import AzDataLakeClient
from azfs.clients.client_interface import ClientInterface
import pandas as pd


class TestClientInterface:
    def test_not_implemented_error(self):
        client_interface = ClientInterface(credential="")
        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.csv"

        with pytest.raises(NotImplementedError):
            client_interface.download_data(path=path)

        with pytest.raises(NotImplementedError):
            client_interface.upload_data(path=path, data={})

        with pytest.raises(NotImplementedError):
            client_interface.ls(path=path)

        with pytest.raises(NotImplementedError):
            client_interface.rm(path=path)

        with pytest.raises(NotImplementedError):
            client_interface.get_properties(path=path)

        with pytest.raises(NotImplementedError):
            client_interface.get_container_client_from_path(path=path)

        with pytest.raises(NotImplementedError):
            client_interface.get_file_client_from_path(path=path)


class TestReadCsv:

    def test_blob_read_csv(self, mocker, _download_data, var_azc):
        mocker.patch.object(AzBlobClient, "_download_data", _download_data)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.csv"

        # read data from not-exist path
        with var_azc:
            df = pd.read_csv_az(path)
        columns = df.columns
        assert "name" in columns
        assert "age" in columns
        assert len(df.index) == 2

    def test_dfs_read_csv(self, mocker, _download_data, var_azc):
        mocker.patch.object(AzDataLakeClient, "_download_data", _download_data)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/test.csv"

        # read data from not-exist path
        with var_azc:
            df = pd.read_csv_az(path)
        columns = df.columns
        assert "name" in columns
        assert "age" in columns
        assert len(df.index) == 2


class TestToCsv:
    def test_blob_to_csv(self, mocker, _upload_data, var_azc, var_df):
        mocker.patch.object(AzBlobClient, "_upload_data", _upload_data)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.csv"

        with var_azc:
            result = var_df.to_csv_az(path)
        assert result

    def test_dfs_to_csv(self, mocker, _upload_data, var_azc, var_df):
        mocker.patch.object(AzDataLakeClient, "_upload_data", _upload_data)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/test.csv"

        with var_azc:
            result = var_df.to_csv_az(path)
        assert result
