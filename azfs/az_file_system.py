from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile

from .core import AzFileClient


class AzFileSystem(AbstractFileSystem):

    def __init__(
            self,
            *args,
            **storage_options):
        super().__init__(*args, **storage_options)

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

    def ls(self, path, detail=True, **kwargs):
        pass

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
