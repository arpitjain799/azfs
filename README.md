# azfs

[![CircleCI](https://circleci.com/gh/gsy0911/azfs.svg?style=svg&circle-token=ccd8e1ece489b247bcaac84861ae725b0f89a605)](https://circleci.com/gh/gsy0911/azfs)
[![codecov](https://codecov.io/gh/gsy0911/azfs/branch/master/graph/badge.svg)](https://codecov.io/gh/gsy0911/azfs)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/gsy0911/azfs.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/gsy0911/azfs/context:python)

[![PythonVersion](https://img.shields.io/badge/python-3.6|3.7|3.8-blue.svg)](https://www.python.org/downloads/release/python-377/)
[![PiPY](https://img.shields.io/badge/pypi-0.1.7-blue.svg)](https://pypi.org/project/azfs/)
[![Downloads](https://pepy.tech/badge/azfs)](https://pepy.tech/project/azfs) 

AzFS is to provide convenient Python read/write functions for Azure Storage Account.

azfs can

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

### create the client

```python
import azfs
from azure.identity import DefaultAzureCredential

# credential is not required if your environment is on AAD
azc = azfs.AzFileClient()

# credential is required if your environment is not on AAD
credential = "[your storage account credential]"
# or
credential = DefaultAzureCredential()
azc = azfs.AzFileClient(credential=credential)

```

#### types of authorization

Currently, only support [Azure Active Directory (AAD) token credential](https://docs.microsoft.com/azure/storage/common/storage-auth-aad).

#### types of storage account kind

The table blow shows if `azfs` provides read/write functions for the storage. 


| account kind | Blob | Data Lake | Queue | File | Table |
|:--|:--:|:--:|:--:|:--:|:--:|
| StorageV2 | O | O | O | X | X |
| StorageV1 | O | O | O | X | X |
| BlobStorage | O | - | - | - | - |

* O: provides basic functions
* X: not provides
* -: storage type unavailable

### download data

azfs can get csv or json data from blob storage.

```python
import azfs
import pandas as pd

azc = azfs.AzFileClient()
csv_path = "https://[storage-account].../*.csv"
json_path = "https://[storage-account].../*.json"
data_path = "https://[storage-account].../*.another_format"

# read csv as pd.DataFrame
df = azc.read_csv(csv_path, index_col=0)
# or
with azc:
    df = pd.read_csv_az(csv_path, header=None)

# read json
data = azc.read_json(json_path)

# also get data directory
data = azc.get(data_path)
# or, (`download` is an alias for `get`) 
data = azc.download(data_path)
```

### upload data

```python
import azfs
import pandas as pd

azc = azfs.AzFileClient()
csv_path = "https://[storage-account].../*.csv"
json_path = "https://[storage-account].../*.json"
data_path = "https://[storage-account].../*.another_format"


df = pd.DataFrame()
data = {"example": "data"}

# write csv
azc.write_csv(path=csv_path, df=df)
# or
with azc:
    df.to_csv_az(path=csv_path, index=False)

# read json as dict
azc.write_json(path=json_path, data=data, indent=4)

# also put data directory
import json
azc.put(path=json_path, data=json.dumps(data, indent=4)) 
# or, (`upload` is an alias for `put`)
azc.upload(path=json_path, data=json.dumps(data, indent=4))
```

### enumerating(ls, glob) or checking if file exists

```python
import azfs

azc = azfs.AzFileClient()

# get file_name list of blob
file_name_list = azc.ls("https://[storage-account].../{container_name}")
# or if set `attach_prefix` True, get full_path list of blob
file_full_path_list = azc.ls("https://[storage-account].../{container_name}", attach_prefix=True)

# find specific file with `*`
file_full_path_list = azc.glob("https://[storage-account].../{container_name}/*.csv")
# also search deeper directory
file_full_path_list = azc.glob("https://[storage-account].../{container_name}/*/*/*.csv")
# or if the directory starts with `a`
file_full_path_list = azc.glob("https://[storage-account].../{container_name}/a*/*.csv")

# check if file exists
is_exists = azc.exists("https://[storage-account].../*.csv")
```

### remove, copy files, etc...

```python
import azfs

azc = azfs.AzFileClient()

# copy file from `src_path` to `dst_path`
src_path = "https://[storage-account].../from/*.csv"
dst_path = "https://[storage-account].../to/*.csv"
is_copied = azc.cp(src_path=src_path, dst_path=dst_path, overwrite=True)

# remove the file
is_removed = azc.rm(path=src_path)

# get file meta info
data = azc.info(path=src_path)

```


## dependencies

```
pandas
azure-identity >= "1.3.1"
azure-storage-blob >= "12.3.0"
azure-storage-file-datalake >= "12.0.0"
azure-storage-queue >= "12.1.1"
```


## references

* [azure-sdk-for-python/storage](https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/storage)
* [filesystem_spec](https://github.com/intake/filesystem_spec)