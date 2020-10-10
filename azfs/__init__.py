from azfs.core import (
    AzFileClient
)

from azfs.az_file_system import AzFileSystem

from azfs.utils import BlobPathDecoder

from .table_storage import (
    TableStorage,
    TableStorageWrapper
)

# comparable tuple
VERSION = (0, 2, 4)
# generate __version__ via VERSION tuple
__version__ = ".".join(map(str, VERSION))
