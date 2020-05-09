import pytest
from azfs.utils import (
    BlobPathDecoder,
    ls_filter
)
from azfs.clients.blob_client import AzBlobClient
import pandas as pd
from azfs.error import (
    AzfsInputError
)


class TestClient:

    def test_blob_read_csv(self, mocker, _download_data, var_azc, var_df):
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
        # assert df == var_df
