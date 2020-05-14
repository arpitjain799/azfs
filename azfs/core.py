import pandas as pd
import json
from azure.identity import DefaultAzureCredential
from azfs.clients import AzfsClient
from azfs.error import AzfsInputError
from typing import Union
from azfs.utils import (
    BlobPathDecoder,
    ls_filter
)
import io


class AzFileClient:
    """
    usage:

    ```
    import azfs
    import pandas as pd

    credential = "[your credential]"
    azc = azfs.AzFileClient(credential=credential)

    path = "your blob file url, starts with https://..."
    with azc:
        df = pd.read_csv_az(path)

    with azc:
        df.to_csv_az(path)

    # ls
    file_list = azc.ls(path)
    ```

    """

    def __init__(
            self,
            credential: Union[str, DefaultAzureCredential, None] = None):
        """

        :param credential: if string, Blob Storage -> Access Keys -> Key
        """
        if credential is None:
            credential = DefaultAzureCredential()
        self.credential = credential

    def __enter__(self):
        """
        with句でのread_csv_azとto_csv_azの関数追加処理
        :return:
        """
        pd.__dict__['read_csv_az'] = self.read_csv
        pd.DataFrame.to_csv_az = self.to_csv(self)
        return self

    def __exit__(self, exec_type, exec_value, traceback):
        """
        with句で追加したread_csv_azとto_csv_azの削除
        :param exec_type:
        :param exec_value:
        :param traceback:
        :return:
        """
        pd.__dict__.pop('read_csv_az')
        pd.DataFrame.to_csv_az = None

    @staticmethod
    def to_csv(az_file_client):
        def inner(self, path, **kwargs):
            df = self if isinstance(self, pd.DataFrame) else None
            return az_file_client.write_csv(path=path, df=df, **kwargs)
        return inner

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
        _, account_kind, _, file_path = BlobPathDecoder(path).get_with_url()
        file_list = AzfsClient.get(account_kind, credential=self.credential).ls(path=path, file_path=file_path)

        return ls_filter(file_path_list=file_list, file_path=file_path)

    def walk(self, path: str, max_depth=2):
        pass

    def cp(self, src_path: str, dst_path: str, overwrite=False):
        """
        copy the data from `src_path` to `dst_path`
        :param src_path:
        :param dst_path:
        :param overwrite:
        :return:
        """
        if src_path == dst_path:
            raise AzfsInputError("src_path and dst_path must be different")
        if (not overwrite) and self.exists(dst_path):
            raise AzfsInputError(f"{dst_path} is already exists. Please set `overwrite=True`.")
        data = self._get(path=src_path)
        if type(data) is io.BytesIO:
            self._put(path=dst_path, data=data.read())
        elif type(data) is bytes:
            self._put(path=dst_path, data=data)
        return True

    def rm(self, path: str) -> bool:
        """
        delete the file in blob
        :param path:
        :return:
        """
        _, account_kind, _, _ = BlobPathDecoder(path).get_with_url()
        return AzfsClient.get(account_kind, credential=self.credential).rm(path=path)

    def info(self, path: str) -> dict:
        """
        get file properties, such as
        * name
        * creation_time
        * last_modified_time
        * size
        * content_hash(md5)
        :param path:
        :return:
        """
        _, account_kind, _, _ = BlobPathDecoder(path).get_with_url()
        # get info from blob or data-lake storage
        data = AzfsClient.get(account_kind, credential=self.credential).info(path=path)

        # extract below to determine file or directory
        content_settings = data.get("content_settings", {})
        metadata = data.get("metadata", {})

        data_type = ""
        if "hdi_isfolder" in metadata:
            # only data-lake storage has `hdi_isfolder`
            data_type = "directory"
        elif content_settings.get("content_type") is not None:
            # blob and data-lake storage have `content_settings`,
            # and its value of the `content_type` must not be None
            data_type = "file"
        return {
            "name": data.get("name", ""),
            "size": data.get("size", ""),
            "creation_time": data.get("creation_time", ""),
            "last_modified": data.get("last_modified", ""),
            "etag": data.get("etag", ""),
            "content_type": content_settings.get("content_type", ""),
            "type": data_type
        }

    def checksum(self, path: str):
        """
        raise KeyError if info has no etag.
        Blob and DataLake storage have etag.
        :param path:
        :return:
        """
        return self.info(path=path)["etag"]

    def size(self, path):
        """
        Size in bytes of file
        """
        return self.info(path).get("size", None)

    def isdir(self, path):
        """
        Is this entry directory-like?
        """
        try:
            return self.info(path)["type"] == "directory"
        except IOError:
            return False

    def isfile(self, path):
        """
        Is this entry file-like?
        """
        try:
            return self.info(path)["type"] == "file"
        except IOError:
            return False

    def glob(self, path: str):
        pass

    def du(self, path):
        pass

    def _get(self, path: str) -> Union[bytes, str, io.BytesIO]:
        """
        storage accountのタイプによってfile_clientを変更し、データを取得する関数
        特定のファイルを取得する関数
        :param path:
        :return:
        """
        _, account_kind, _, _ = BlobPathDecoder(path).get_with_url()
        return AzfsClient.get(account_kind, credential=self.credential).get(path=path)

    def read_csv(self, path: str, **kwargs) -> pd.DataFrame:
        """
        blobにあるcsvを読み込み、pd.DataFrameとして取得する関数。
        gzip圧縮にも対応。
        :param path:
        :return:
        """
        file_to_read = self._get(path)
        return pd.read_csv(file_to_read, **kwargs)

    def _put(self, path: str, data) -> bool:
        """
        upload data to blob or data_lake storage account
        :param path:
        :param data:
        :return:
        """
        _, account_kind, _, _ = BlobPathDecoder(path).get_with_url()
        return AzfsClient.get(account_kind, credential=self.credential).put(path=path, data=data)

    def write_csv(self, path: str, df: pd.DataFrame, **kwargs) -> bool:
        """
        output pandas dataframe to csv file in Datalake storage.
        Note: Unavailable for large loop processing!
        """
        csv_str = df.to_csv(encoding="utf-8", **kwargs)
        return self._put(path=path, data=csv_str)

    def read_json(self, path: str, **kwargs) -> dict:
        """
        read json file in Datalake storage.
        Note: Unavailable for large loop processing!
        """
        file_bytes = self._get(path)
        if type(file_bytes) is io.BytesIO:
            file_bytes = file_bytes.read()
        return json.loads(file_bytes, **kwargs)

    def write_json(self, path: str, data: dict, **kwargs) -> bool:
        """
        output dict to json file in Datalake storage.
        Note: Unavailable for large loop processing!
        """
        return self._put(path=path, data=json.dumps(data, **kwargs))

    # ===================
    # alias for functions
    # ===================

    get = _get
    download = _get
    put = _put
    upload = _put
