from typing import Union, Optional

from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from azure.identity import DefaultAzureCredential

from .core import AzFileClient
from .utils import (
    BlobPathDecoder,
    ls_filter
)
from .clients import AzfsClient


class AzFileSystem(AbstractFileSystem):

    def __init__(
            self,
            credential: Optional[Union[str, DefaultAzureCredential]] = None,
            connection_string: str = None,
            *args,
            **storage_options):
        super().__init__(*args, **storage_options)
        if credential is None and connection_string is None:
            credential = DefaultAzureCredential()
        self._client = AzfsClient(credential=credential, connection_string=connection_string)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs
    ):
        """Return raw bytes-mode file-like from the file-system"""
        return AzFile(
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            **kwargs
        )

    def ls(self, path, detail=True, attach_prefix=False, **kwargs):
        _, account_kind, _, file_path = BlobPathDecoder(path).get_with_url()
        file_list = self._client.get_client(account_kind=account_kind).ls(path=path, file_path=file_path)
        if account_kind in ["dfs", "blob"]:
            file_name_list = ls_filter(file_path_list=file_list, file_path=file_path)
            if attach_prefix:
                path = path if path.endswith("/") else f"{path}/"
                file_full_path_list = [f"{path}{f}" for f in file_name_list]
                return file_full_path_list
            else:
                return file_name_list

    def copy(self, path1, path2, **kwargs):
        pass

    def _rm(self, path):
        pass

    def created(self, path):
        pass

    def modified(self, path):
        pass


class AzFile(AbstractBufferedFile):

    def __init__(
            self,
            fs,
            path,
            mode="rb",
            block_size="default",
            autocommit=True,
            cache_type="readahead",
            cache_options=None,
            **kwargs):
        super().__init__(fs, path, mode, block_size, autocommit, cache_type, cache_options, **kwargs)

    def _fetch_range(self, start, end):
        pass
