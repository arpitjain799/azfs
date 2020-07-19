import pytest
from azure.core.exceptions import ResourceNotFoundError
from azfs.clients.blob_client import AzBlobClient
from azfs.clients.datalake_client import AzDataLakeClient
from azfs.clients.client_interface import ClientInterface
from azfs.error import AzfsInputError
import pandas as pd


class TestClientInterface:
    def test_not_implemented_error(self):
        client_interface = ClientInterface(credential="")
        # the file below is not exists
        account_url = "https://testazfs.blob.core.windows.net/"
        path = f"{account_url}test_caontainer/test.csv"
        file_path = "test_caontainer"

        with pytest.raises(NotImplementedError):
            client_interface.get(path=path)

        with pytest.raises(NotImplementedError):
            client_interface.put(path=path, data={})

        with pytest.raises(NotImplementedError):
            client_interface.ls(path=path, file_path=file_path)

        with pytest.raises(NotImplementedError):
            client_interface.rm(path=path)

        with pytest.raises(NotImplementedError):
            client_interface.info(path=path)

        with pytest.raises(NotImplementedError):
            client_interface.get_container_client_from_path(path=path)

        with pytest.raises(NotImplementedError):
            client_interface.get_file_client_from_path(path=path)

        with pytest.raises(NotImplementedError):
            client_interface.get_service_client_from_url(account_url=account_url)

        # connection_stringから作成する場合
        client_interface = ClientInterface(credential=None, connection_string="")
        with pytest.raises(NotImplementedError):
            client_interface.get_service_client_from_url(account_url=account_url)


class TestReadCsv:

    def test_blob_read_csv(self, mocker, _get_csv, var_azc):
        mocker.patch.object(AzBlobClient, "_get", _get_csv)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.csv"

        # read data from not-exist path
        with var_azc:
            df = pd.read_csv_az(path)
        columns = df.columns
        assert "name" in columns
        assert "age" in columns
        assert len(df.index) == 2

    def test_blob_read_csv_gz(self, mocker, _get_csv_gz, var_azc):
        mocker.patch.object(AzBlobClient, "_get", _get_csv_gz)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.csv.gz"

        # read data from not-exist path
        with var_azc:
            df = pd.read_csv_az(path)
        columns = df.columns
        assert "name" in columns
        assert "age" in columns
        assert len(df.index) == 2

    def test_dfs_read_csv(self, mocker, _get_csv, var_azc):
        mocker.patch.object(AzDataLakeClient, "_get", _get_csv)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/test.csv"

        # read data from not-exist path
        with var_azc:
            df = pd.read_csv_az(path)
        columns = df.columns
        assert "name" in columns
        assert "age" in columns
        assert len(df.index) == 2


class TestReadTable:

    def test_blob_read_table(self, mocker, _get_table, var_azc):
        mocker.patch.object(AzBlobClient, "_get", _get_table)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.tsv"

        # read data from not-exist path
        with var_azc:
            df = pd.read_table_az(path)
        columns = df.columns
        assert "name" in columns
        assert "age" in columns
        assert len(df.index) == 2


class TestReadPickle:

    def test_blob_read_pickle(self, mocker, _get_pickle, var_azc):
        mocker.patch.object(AzBlobClient, "_get", _get_pickle)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.pkl"

        # read data from not-exist path
        with var_azc:
            df = pd.read_pickle_az(path, compression=None)
        columns = df.columns
        assert "name" in columns
        assert "age" in columns
        assert len(df.index) == 2

    def test_blob_read_pickle_gzip(self, mocker, _get_pickle_gzip, var_azc):
        mocker.patch.object(AzBlobClient, "_get", _get_pickle_gzip)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.pkl"

        # read data from not-exist path
        with var_azc:
            df = pd.read_pickle_az(path, compression="gzip")
        columns = df.columns
        assert "name" in columns
        assert "age" in columns
        assert len(df.index) == 2

    def test_blob_read_pickle_bz2(self, mocker, _get_pickle_bz2, var_azc):
        mocker.patch.object(AzBlobClient, "_get", _get_pickle_bz2)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.pkl"

        # read data from not-exist path
        with var_azc:
            df = pd.read_pickle_az(path, compression="bz2")
        columns = df.columns
        assert "name" in columns
        assert "age" in columns
        assert len(df.index) == 2

    def test_blob_read_pickle_xz(self, mocker, _get_pickle_xz, var_azc):
        mocker.patch.object(AzBlobClient, "_get", _get_pickle_xz)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.pkl"

        # read data from not-exist path
        with var_azc:
            df = pd.read_pickle_az(path, compression="xz")
        columns = df.columns
        assert "name" in columns
        assert "age" in columns
        assert len(df.index) == 2


class TestReadJson:

    def test_blob_read_json(self, mocker, _get_json, var_azc, var_json):
        mocker.patch.object(AzBlobClient, "_get", _get_json)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.json"

        data = var_azc.read_json(path)
        assert data == var_json

    def test_dfs_read_json(self, mocker, _get_json, var_azc, var_json):
        mocker.patch.object(AzDataLakeClient, "_get", _get_json)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/test.json"

        data = var_azc.read_json(path)
        assert data == var_json


class TestReadLineIter:

    def test_blob_read_line_iter(self, mocker, _get_csv, var_azc):
        mocker.patch.object(AzBlobClient, "_get", _get_csv)

        return_value = {"size": len(b'name,age\nalice,10\nbob,10\n')}
        func_mock = mocker.MagicMock()
        func_mock.return_value = return_value
        mocker.patch.object(AzBlobClient, "_info", func_mock)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.csv"

        # read data from not-exist path
        line_counter = 0
        for _ in var_azc.read_line_iter(path=path):
            line_counter += 1

        assert line_counter == 3

    def test_dfs_read_line_iter(self, mocker, _get_csv, var_azc):
        mocker.patch.object(AzDataLakeClient, "_get", _get_csv)

        return_value = {"size": len(b'name,age\nalice,10\nbob,10\n')}
        func_mock = mocker.MagicMock()
        func_mock.return_value = return_value
        mocker.patch.object(AzDataLakeClient, "_info", func_mock)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/test.csv"

        # read data from not-exist path
        line_counter = 0
        for _ in var_azc.read_line_iter(path=path):
            line_counter += 1

        assert line_counter == 3


class TestReadCsvChunk:
    def test_blob_read_csv_chunk(self, mocker, _get_csv, var_azc):
        mocker.patch.object(AzBlobClient, "_get", _get_csv)
        return_value = {"size": len(b'name,age\nalice,10\nbob,10\n')}
        func_mock = mocker.MagicMock()
        func_mock.return_value = return_value
        mocker.patch.object(AzBlobClient, "_info", func_mock)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.csv"
        chunk_size = 2
        chunk_counter = 0
        with pytest.warns(FutureWarning):
            for _ in var_azc.read_csv_chunk(path, chunk_size):
                chunk_counter += 1
            assert chunk_counter == 2

    def test_dfs_read_csv_chunk(self, mocker, _get_csv, var_azc):
        mocker.patch.object(AzDataLakeClient, "_get", _get_csv)
        return_value = {"size": len(b'name,age\nalice,10\nbob,10\n')}
        func_mock = mocker.MagicMock()
        func_mock.return_value = return_value
        mocker.patch.object(AzDataLakeClient, "_info", func_mock)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/test.csv"
        chunk_size = 2
        chunk_counter = 0
        with pytest.warns(FutureWarning):
            for _ in var_azc.read_csv_chunk(path, chunk_size):
                chunk_counter += 1
            assert chunk_counter == 2


class TestToCsv:
    def test_blob_to_csv(self, mocker, _put, var_azc, var_df):
        mocker.patch.object(AzBlobClient, "_put", _put)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.csv"

        with var_azc:
            result = var_df.to_csv_az(path)
        assert result

    def test_dfs_to_csv(self, mocker, _put, var_azc, var_df):
        mocker.patch.object(AzDataLakeClient, "_put", _put)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/test.csv"

        with var_azc:
            result = var_df.to_csv_az(path)
        assert result


class TestToTsv:
    def test_blob_to_table(self, mocker, _put, var_azc, var_df):
        mocker.patch.object(AzBlobClient, "_put", _put)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.tsv"

        with var_azc:
            result = var_df.to_table_az(path)
        assert result

    def test_dfs_to_table(self, mocker, _put, var_azc, var_df):
        mocker.patch.object(AzDataLakeClient, "_put", _put)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/test.tsv"

        with var_azc:
            result = var_df.to_table_az(path)
        assert result


class TestToPickle:
    def test_blob_to_pickle(self, mocker, _put, var_azc, var_df):
        mocker.patch.object(AzBlobClient, "_put", _put)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.pkl"

        with var_azc:
            result = var_df.to_pickle_az(path)
            assert result
            result = var_df.to_pickle_az(path, compression=None)
            assert result
            result = var_df.to_pickle_az(path, compression="gzip")
            assert result
            result = var_df.to_pickle_az(path, compression="bz2")
            assert result
            result = var_df.to_pickle_az(path, compression="xz")
            assert result

    def test_dfs_to_pickle(self, mocker, _put, var_azc, var_df):
        mocker.patch.object(AzDataLakeClient, "_put", _put)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/test.pkl"

        with var_azc:
            result = var_df.to_pickle_az(path)
            assert result
            result = var_df.to_pickle_az(path, compression=None)
            assert result
            result = var_df.to_pickle_az(path, compression="gzip")
            assert result
            result = var_df.to_pickle_az(path, compression="bz2")
            assert result
            result = var_df.to_pickle_az(path, compression="xz")
            assert result


class TestToJson:

    def test_blob_to_csv(self, mocker, _put, var_azc):
        mocker.patch.object(AzBlobClient, "_put", _put)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test.json"

        result = var_azc.write_json(path, data={"a": "b"})
        assert result

    def test_dfs_to_csv(self, mocker, _put, var_azc):
        mocker.patch.object(AzDataLakeClient, "_put", _put)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/test.json"

        result = var_azc.write_json(path, data={"a": "b"})
        assert result


class TestLs:
    def test_blob_ls(self, mocker, _ls, var_azc):
        mocker.patch.object(AzBlobClient, "_ls", _ls)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/"

        file_list = var_azc.ls(path=path)
        assert len(file_list) == 3
        assert "test1.csv" in file_list
        assert "test2.csv" in file_list
        assert "dir/" in file_list

    def test_blob_ls_full_path(self, mocker, _ls, var_azc):
        mocker.patch.object(AzBlobClient, "_ls", _ls)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/"

        file_list = var_azc.ls(path=path, attach_prefix=True)
        assert len(file_list) == 3
        assert "https://testazfs.blob.core.windows.net/test_caontainer/test1.csv" in file_list
        assert "https://testazfs.blob.core.windows.net/test_caontainer/test2.csv" in file_list
        assert "https://testazfs.blob.core.windows.net/test_caontainer/dir/" in file_list

    def test_dfs_ls(self, mocker, _ls, var_azc):
        mocker.patch.object(AzDataLakeClient, "_ls", _ls)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/"

        file_list = var_azc.ls(path=path)
        assert len(file_list) == 3
        assert "test1.csv" in file_list
        assert "test2.csv" in file_list
        assert "dir/" in file_list

    def test_dfs_ls_full_path(self, mocker, _ls, var_azc):
        mocker.patch.object(AzDataLakeClient, "_ls", _ls)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/"

        file_list = var_azc.ls(path=path, attach_prefix=True)
        assert len(file_list) == 3
        assert "https://testazfs.dfs.core.windows.net/test_caontainer/test1.csv" in file_list
        assert "https://testazfs.dfs.core.windows.net/test_caontainer/test2.csv" in file_list
        assert "https://testazfs.dfs.core.windows.net/test_caontainer/dir/" in file_list


class TestGlob:
    def test_blob_glob_error(self, var_azc):
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test1.csv"
        with pytest.raises(AzfsInputError):
            var_azc.glob(path)
        path = "https://testazfs.blob.core.windows.net/test_caontainer/*"
        with pytest.raises(AzfsInputError):
            var_azc.glob(path)

    def test_blob_glob(self, mocker, _ls_for_glob, var_azc):
        mocker.patch.object(AzBlobClient, "_ls", _ls_for_glob)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/root_folder/*.csv"
        file_list = var_azc.glob(pattern_path=path)
        assert len(file_list) == 2
        assert "https://testazfs.blob.core.windows.net/test_caontainer/root_folder/test1.csv" in file_list
        assert "https://testazfs.blob.core.windows.net/test_caontainer/root_folder/test2.csv" in file_list

        path = "https://testazfs.blob.core.windows.net/test_caontainer/root_folder/*.json"
        file_list = var_azc.glob(pattern_path=path)
        assert len(file_list) == 1
        assert "https://testazfs.blob.core.windows.net/test_caontainer/root_folder/test1.json" in file_list

        path = "https://testazfs.blob.core.windows.net/test_caontainer/root_folder/*/*.csv"
        file_list = var_azc.glob(pattern_path=path)
        assert len(file_list) == 4
        assert "https://testazfs.blob.core.windows.net/test_caontainer/root_folder/dir1/test1.csv" in file_list
        assert "https://testazfs.blob.core.windows.net/test_caontainer/root_folder/dir1/test2.csv" in file_list
        assert "https://testazfs.blob.core.windows.net/test_caontainer/root_folder/dir2/test1.csv" in file_list
        assert "https://testazfs.blob.core.windows.net/test_caontainer/root_folder/dir2/test2.csv" in file_list

        path = "https://testazfs.blob.core.windows.net/test_caontainer/root_folder/dir1/*.csv"
        file_list = var_azc.glob(pattern_path=path)
        assert len(file_list) == 2
        assert "https://testazfs.blob.core.windows.net/test_caontainer/root_folder/dir1/test1.csv" in file_list
        assert "https://testazfs.blob.core.windows.net/test_caontainer/root_folder/dir1/test2.csv" in file_list

    def test_dfs_glob_error(self, var_azc):
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/test1.csv"
        with pytest.raises(AzfsInputError):
            var_azc.glob(path)
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/*"
        with pytest.raises(AzfsInputError):
            var_azc.glob(path)

    def test_dfs_glob(self, mocker, _ls_for_glob, var_azc):
        mocker.patch.object(AzDataLakeClient, "_ls", _ls_for_glob)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/*.csv"
        file_list = var_azc.glob(pattern_path=path)
        assert len(file_list) == 2
        assert "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/test1.csv" in file_list
        assert "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/test2.csv" in file_list

        path = "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/*.json"
        file_list = var_azc.glob(pattern_path=path)
        assert len(file_list) == 1
        assert "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/test1.json" in file_list

        path = "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/*/*.csv"
        file_list = var_azc.glob(pattern_path=path)
        assert len(file_list) == 4
        assert "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/dir1/test1.csv" in file_list
        assert "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/dir1/test2.csv" in file_list
        assert "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/dir2/test1.csv" in file_list
        assert "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/dir2/test2.csv" in file_list

        path = "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/dir1/*.csv"
        file_list = var_azc.glob(pattern_path=path)
        assert len(file_list) == 2
        assert "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/dir1/test1.csv" in file_list
        assert "https://testazfs.dfs.core.windows.net/test_caontainer/root_folder/dir1/test2.csv" in file_list


class TestRm:
    def test_blob_rm(self, mocker, _rm, var_azc):
        mocker.patch.object(AzBlobClient, "_rm", _rm)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/"

        result = var_azc.rm(path=path)
        assert result

    def test_dfs_rm(self, mocker, _rm, var_azc):
        mocker.patch.object(AzDataLakeClient, "_rm", _rm)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/"

        result = var_azc.rm(path=path)
        assert result


class TestExists:
    def test_blob_exists(self, mocker, _get_csv, var_azc):
        mocker.patch.object(AzBlobClient, "_get", _get_csv)

        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test1.csv"

        result = var_azc.exists(path=path)
        assert result

        # set to raise exception
        _get_csv.side_effect = ResourceNotFoundError
        mocker.patch.object(AzBlobClient, "_get", _get_csv)
        # the file below is not exists
        path = "https://testazfs.blob.core.windows.net/test_caontainer/test3.csv"
        result = var_azc.exists(path=path)
        assert not result

    def test_dfs_exists(self, mocker, _get_csv, var_azc):
        mocker.patch.object(AzDataLakeClient, "_get", _get_csv)

        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/test1.csv"

        result = var_azc.exists(path=path)
        assert result

        # set to raise exception
        _get_csv.side_effect = ResourceNotFoundError
        mocker.patch.object(AzDataLakeClient, "_get", _get_csv)
        # the file below is not exists
        path = "https://testazfs.dfs.core.windows.net/test_caontainer/test3.csv"
        result = var_azc.exists(path=path)
        assert not result
