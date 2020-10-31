import pytest
from azfs.clients.blob_client import AzBlobClient
from azure.storage.blob import BlobClient, ContainerClient
import azfs

credential = ""
connection_string = "DefaultEndpointsProtocol=https;AccountName=xxxx;AccountKey=xxxx;EndpointSuffix=core.windows.net"
azc = azfs.AzFileClient()
blob_client_credential = AzBlobClient(credential=credential)
blob_client_connection_string = AzBlobClient(credential=None, connection_string=connection_string)
test_file_path = "https://test.blob.core.windows.net/test/test.csv"
test_file_ls_path = "https://test.blob.core.windows.net/test/"


