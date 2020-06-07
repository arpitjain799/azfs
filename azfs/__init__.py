from azfs.core import (
    AzFileClient
)

from azfs.utils import BlobPathDecoder

# comparable tuple
VERSION = (0, 1, 7)
# generate __version__ via VERSION tuple
__version__ = ".".join(map(str, VERSION))
