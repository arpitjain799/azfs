# AzFS

[![CircleCI](https://circleci.com/gh/gsy0911/azfs.svg?style=svg&circle-token=ccd8e1ece489b247bcaac84861ae725b0f89a605)](https://circleci.com/gh/gsy0911/azfs)
[![codecov](https://codecov.io/gh/gsy0911/azfs/branch/master/graph/badge.svg)](https://codecov.io/gh/gsy0911/azfs)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/gsy0911/azfs.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/gsy0911/azfs/context:python)
[![Documentation Status](https://readthedocs.org/projects/azfs/badge/?version=latest)](https://azfs.readthedocs.io/en/latest/?badge=latest)


[![PythonVersion](https://img.shields.io/badge/python-3.6|3.7|3.8-blue.svg)](https://www.python.org/downloads/release/python-377/)
[![PiPY](https://img.shields.io/pypi/v/azfs.svg)](https://pypi.org/project/azfs/)
[![Downloads](https://pepy.tech/badge/azfs)](https://pepy.tech/project/azfs) 

AzFS is to provide convenient Python read/write functions for Azure Storage Account.

`AzFS` can

* list files in blob (also with wildcard `*`),
* check if file exists,
* read csv as pd.DataFrame, and json as dict from blob,
* write pd.DataFrame as csv, and dict as json to blob,
* and raise lots of exceptions ! (Thank you for your cooperation)

## install

```bash
$ pip install azfs
```

## usage

For `Blob` and `Queue` Storage.


```python
import azfs
from azure.identity import DefaultAzureCredential
import pandas as pd

# credential is not required if your environment is on AAD(Azure Active Directory)
azc = azfs.AzFileClient()

# credential is required if your environment is not on AAD
credential = "[your storage account credential]"
# or
credential = DefaultAzureCredential()
azc = azfs.AzFileClient(credential=credential)

# connection_string is also supported
connection_string = "DefaultEndpointsProtocol=https;AccountName=xxxx;AccountKey=xxxx;EndpointSuffix=core.windows.net"
azc = azfs.AzFileClient(connection_string=connection_string)

# data paths
csv_path = "https://testazfs.blob.core.windows.net/test_caontainer/test_file.csv"

# read csv as pd.DataFrame
df = azc.read_csv(csv_path, index_col=0)
# or
with azc:
    df = pd.read_csv_az(csv_path, header=None)


# write csv
azc.write_csv(path=csv_path, df=df)
# or
with azc:
    df.to_csv_az(path=csv_path, index=False)

```

For `Table` Storage

```python

import azfs
cons = {
    "account_name": "{storage_account_name}",
    "account_key": "{credential}",
    "database_name": "{database_name}"
}

table_client = azfs.TableStorageWrapper(**cons)

# put data, according to the keyword you put
table_client.put(id_="1", message="hello_world")

# get data
table_client.get(id_="1")

```

check more details in  [![Documentation Status](https://readthedocs.org/projects/azfs/badge/?version=latest)](https://azfs.readthedocs.io/en/latest/?badge=latest)

### types of authorization

Supported authentication types are
* [Azure Active Directory (AAD) token credential](https://docs.microsoft.com/azure/storage/common/storage-auth-aad).
* connection_string, like `DefaultEndpointsProtocol=https;AccountName=xxxx;AccountKey=xxxx;EndpointSuffix=core.windows.net` 

### types of storage account kind

The table blow shows if `AzFS` provides read/write functions for the storage. 


| account kind | Blob | Data Lake | Queue | File | Table |
|:--|:--:|:--:|:--:|:--:|:--:|
| StorageV2 | O | O | O | X | O |
| StorageV1 | O | O | O | X | O |
| BlobStorage | O | - | - | - | - |

* O: provides basic functions
* X: not provides
* -: storage type unavailable

## dependencies

```
pandas
azure-identity >= "1.3.1"
azure-storage-blob >= "12.3.0"
azure-storage-file-datalake >= "12.0.0"
azure-storage-queue >= "12.1.1"
azure-cosmosdb-table
```


## references

* [azure-sdk-for-python/storage](https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/storage)
* [filesystem_spec](https://github.com/intake/filesystem_spec)