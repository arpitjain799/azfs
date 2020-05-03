import pytest
from azfs.utils import (
    BlobPathDecoder,
    ls_filter
)
from azfs.error import (
    AzfsInputError
)


class TestBlobPathDecoder:

    @pytest.mark.parametrize("path,storage_account_name,account_type,container_name,blob_file", [
        ("https://test.blob.core.windows.net/test/test_file.csv", "test", "blob", "test", "test_file.csv"),
        ("https://test.blob.core.windows.net/test/dir/test_file.csv", "test", "blob", "test", "dir/test_file.csv"),
        ("https://test.blob.core.windows.net/test/", "test", "blob", "test", ""),
        ("https://test.blob.core.windows.net/test", "test", "blob", "test", ""),
        ("https://test.blob.core.windows.net/", "test", "blob", "", ""),
        ("blob/test/test/test_file.csv", "test", "blob", "test", "test_file.csv"),
    ])
    def test_path_decoder_pass(self, path, storage_account_name, account_type, container_name, blob_file):
        bpd = BlobPathDecoder()
        storage_account_name_v, account_type_v, container_name_v, blob_file_v = bpd.decode(path).get()
        assert storage_account_name_v == storage_account_name
        assert account_type_v == account_type
        assert container_name_v == container_name
        assert blob_file_v == blob_file

        storage_account_name_v, account_type_v, container_name_v, blob_file_v = BlobPathDecoder(path).get()
        assert storage_account_name_v == storage_account_name
        assert account_type_v == account_type
        assert container_name_v == container_name
        assert blob_file_v == blob_file

    def test_path_decoder_error(self):
        path = "https://aaa/bbb/ccc"

        bpd = BlobPathDecoder()
        with pytest.raises(AzfsInputError):
            bpd.decode(path)

        with pytest.raises(AzfsInputError):
            BlobPathDecoder(path)


class TestLsFilter:
    def test_ls_filter(self):
        file_path_list = [
            "file1.csv",
            "file2.csv",
            "file3.csv",
            "dir/file4.csv",
            "dir/file5.csv",
            "dir/some/file6.csv"
        ]
        result_list = [
            "file1.csv",
            "file2.csv",
            "file3.csv",
            "dir/"
        ]
        result_list_v = ls_filter(file_path_list, "")
        assert result_list_v == result_list

        result_list = [
            "file4.csv",
            "file5.csv",
            "some/"
        ]
        result_list_v = ls_filter(file_path_list, "dir")
        assert result_list_v == result_list
