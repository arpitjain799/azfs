import pandas as pd
import gzip
import io
import json
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import (
    DataLakeFileClient
)
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    ContainerClient
)
from typing import Union
from azfs.error import (
    AzfsInputError
)
from azfs.utils import (
    BlobPathDecoder,
    ls_filter
)


class AzFileClient:
    """

    """

    def __init__(
            self,
            credential: Union[str, DefaultAzureCredential],
            *,
            storage_account_name: str = None,
            account_url: str = None):
        """

        :param credential: if string, Blob Storage -> Access Keys -> Key
        """
        self.credential = credential

        # generate ServiceClient
        self.account_url = None
        if not (storage_account_name is None or account_url is None):
            # self.url_pattern = None
            raise AzfsInputError("両方の値を設定することはできません")
        elif storage_account_name is not None:
            self.account_url = f"https://{storage_account_name}.blob.core.windows.net"
        elif account_url is not None:
            self.account_url = account_url

        # ServiceClient
        self.service_client: Union[BlobServiceClient, None] = None
        if self.account_url is not None:
            self.service_client = BlobServiceClient(account_url=self.account_url, credential=credential)

    def __enter__(self):
        pd.__dict__['read_csv_az'] = self.read_csv
        pd.DataFrame.to_csv_az = self.to_csv(self)
        return self

    def __exit__(self, exec_type, exec_value, traceback):
        """
        restore pandas existing function
        :param exec_type:
        :param exec_value:
        :param traceback:
        :return:
        """
        pd.__dict__.pop('read_csv_az')
        pd.DataFrame.to_csv_az = None

    @staticmethod
    def to_csv(azc):
        def inner(self, path, **kwargs):
            df = self if isinstance(self, pd.DataFrame) else None
            return azc.write_csv(path=path, df=df, **kwargs)
        return inner

    @staticmethod
    def _get_file_client(
            storage_account_url,
            account_kind,
            file_system,
            file_path,
            credential: DefaultAzureCredential) -> Union[DataLakeFileClient, BlobClient]:
        """

        :param storage_account_url:
        :param account_kind:
        :param file_system:
        :param file_path:
        :param credential:
        :return:
        """
        if account_kind == "dfs":
            file_client = DataLakeFileClient(
                storage_account_url,
                file_system,
                file_path,
                credential=credential)
            return file_client
        elif account_kind == "blob":
            file_client = BlobClient(
                account_url=storage_account_url,
                container_name=file_system,
                blob_name=file_path,
                credential=credential
            )
            return file_client
        else:
            raise AzfsInputError("account_kindが不正です")

    def exists(self, path: str) -> bool:
        # 親パスの部分を取得
        parent_path = path.rsplit("/", 1)[0]
        file_name = path.rsplit("/", 1)[1]
        file_list = self.ls(parent_path)
        if file_list:
            if file_name in file_list:
                return True
        return False

    def ls(self, path: str):
        """
        list blob file
        :param path:
        :return:
        """
        storage_account_url, account_kind, file_system, file_path = BlobPathDecoder(path).get_with_url()
        if self.service_client is None:
            container_client = ContainerClient(
                account_url=storage_account_url,
                container_name=file_system,
                credential=self.credential)
        else:
            container_client = self.service_client.get_container_client(file_system)

        # container以下のフォルダを取得する
        blob_list = [f.name for f in container_client.list_blobs()]
        return ls_filter(file_path_list=blob_list, file_path=file_path)

    def _download_data(self, path: str) -> Union[bytes, str]:
        """
        storage accountのタイプによってfile_clientを変更し、データを取得する関数
        特定のファイルを取得する関数
        :param path:
        :return:
        """
        storage_account_url, account_kind, file_system, file_path = BlobPathDecoder(path).get_with_url()
        file_bytes = None
        if account_kind == "dfs":
            file_client = self._get_file_client(
                storage_account_url=storage_account_url,
                account_kind=account_kind,
                file_system=file_system,
                file_path=file_path,
                credential=self.credential)
            file_bytes = file_client.download_file().readall()
        elif account_kind == "blob":
            file_client = self._get_file_client(
                storage_account_url=storage_account_url,
                account_kind=account_kind,
                file_system=file_system,
                file_path=file_path,
                credential=self.credential)
            file_bytes = file_client.download_blob().readall()

        # gzip圧縮ファイルは一旦ここで展開
        if file_path.endswith(".gz"):
            file_bytes = gzip.decompress(file_bytes)
        return file_bytes

    def read_csv(self, path: str, **kwargs) -> pd.DataFrame:
        """
        blobにあるcsvを読み込み、pd.DataFrameとして取得する関数。
        gzip圧縮にも対応。
        :param path:
        :return:
        """
        file_bytes = self._download_data(path)
        if type(file_bytes) is bytes:
            file_to_read = io.BytesIO(file_bytes)
        else:
            file_to_read = file_bytes
        return pd.read_csv(file_to_read, **kwargs)

    def _upload_data(self, path: str, data):
        """
        upload data to blob or data_lake storage account
        :param path:
        :param data:
        :return:
        """
        storage_account_url, account_kind, file_system, file_path = BlobPathDecoder(path).get_with_url()
        file_client = self._get_file_client(
            storage_account_url=storage_account_url,
            account_kind=account_kind,
            file_system=file_system,
            file_path=file_path,
            credential=self.credential)
        if account_kind == "dfs":
            _ = file_client.create_file()
            _ = file_client.append_data(data=data, offset=0, length=len(data))
            _ = file_client.flush_data(len(data))
            return True
        elif account_kind == "blob":
            file_client.upload_blob(data=data, length=len(data))
        return True

    def write_csv(self, path: str, df: pd.DataFrame, **kwargs) -> bool:
        """
        output pandas dataframe to csv file in Datalake storage.
        Note: Unavailable for large loop processing!
        """
        csv_str = df.to_csv(encoding="utf-8", **kwargs)
        return self._upload_data(path=path, data=csv_str)

    def read_json(self, path: str) -> dict:
        """
        read json file in Datalake storage.
        Note: Unavailable for large loop processing!
        """
        file_bytes = self._download_data(path)
        return json.loads(file_bytes)

    def write_json(self, path: str, data: dict) -> bool:
        """
        output dict to json file in Datalake storage.
        Note: Unavailable for large loop processing!
        """
        return self._upload_data(path=path, data=json.dumps(data))
