import pytest
from azfs.utils import BlobPathDecoder
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
        ("test/test/test_file.csv", "test", "", "test", "test_file.csv"),
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
