from azfs.az_file_client import (
    AzFileClient
)

from azfs.az_file_system import AzFileSystem

from azfs.utils import BlobPathDecoder

from .table_storage import (
    TableStorage,
    TableStorageWrapper
)

# comparable tuple
VERSION = (0, 2, 7)
# generate __version__ via VERSION tuple
__version__ = ".".join(map(str, VERSION))

__all__ = ["AzFileClient", "AzFileSystem", "BlobPathDecoder", "TableStorage", "TableStorageWrapper"]
