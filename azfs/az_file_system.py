from typing import Union, Optional

from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from azure.identity import DefaultAzureCredential

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
        self.az_client = AzfsClient(credential=credential, connection_string=connection_string)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs
    ):
        """
        (Override method)
        Return raw bytes-mode file-like from the file-system

        """
        return AzFile(
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            **kwargs
        )

    def info(self, path, **kwargs):
        """
        (Override method)

        Args:
            path:
            **kwargs:

        Returns:

        """
        _, account_kind, _, _ = BlobPathDecoder(path).get_with_url()
        # get info from blob or data-lake storage
        data = self.az_client.get_client(account_kind=account_kind).info(path=path)

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

    def ls(self, path, detail=True, attach_prefix=False, **kwargs):
        """
        (Inherited method)

        Args:
            path:
            detail:
            attach_prefix:
            **kwargs:

        Returns:

        """
        _, account_kind, _, file_path = BlobPathDecoder(path).get_with_url()
        file_list = self.az_client.get_client(account_kind=account_kind).ls(path=path, file_path=file_path)
        if account_kind in ["dfs", "blob"]:
            file_name_list = ls_filter(file_path_list=file_list, file_path=file_path)
            if attach_prefix:
                path = path if path.endswith("/") else f"{path}/"
                file_full_path_list = [f"{path}{f}" for f in file_name_list]
                return file_full_path_list
            else:
                return file_name_list

    def copy(self, path1, path2, **kwargs):
        """
        (Inherited method)


        Args:
            path1:
            path2:
            **kwargs:

        Returns:

        """
        pass

    def _rm(self, path):
        """
        (Inherited method)

        Args:
            path:

        Returns:

        """
        pass

    def created(self, path):
        """
        (Inherited method)


        Args:
            path:

        Returns:

        """
        pass

    def modified(self, path):
        """
        (Inherited method)

        Args:
            path:

        Returns:

        """
        pass


class AzFile(AbstractBufferedFile):
    """

    Args:
        fs: AzFileSystem
        path: path to read file
        mode: value candidates are same as build-in function `open()`
        block_size:
        autocommit:
        cache_type:
        cache_option:
    """

    def __init__(
            self,
            fs: AzFileSystem,
            path: str,
            mode="rb",
            block_size="default",
            autocommit=True,
            cache_type="readahead",
            cache_options=None,
            **kwargs):
        super().__init__(fs, path, mode, block_size, autocommit, cache_type, cache_options, **kwargs)
        self.fs = fs
        self.path = path

        # check block_size from AbstractBufferedFile
        if self.writable():
            if block_size < 5 * 2 ** 20:
                raise ValueError('Block size must be >=5MB')

    def _fetch_range(self, start, end):
        _, account_kind, _, file_path = BlobPathDecoder(self.path).get_with_url()
        return self.fs.az_client.get_client(
            account_kind=account_kind).get(path=self.path, offset=start, length=end-start)
