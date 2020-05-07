# azfs

[![CircleCI](https://circleci.com/gh/gsy0911/azfs.svg?style=svg&circle-token=ccd8e1ece489b247bcaac84861ae725b0f89a605)](https://circleci.com/gh/gsy0911/azfs)
[![codecov](https://codecov.io/gh/gsy0911/azfs/branch/master/graph/badge.svg)](https://codecov.io/gh/gsy0911/azfs)

AzFS is to provide convenient Python read/write functions for Azure Storage Account.

azfs can

* list files in blob,
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

# credential is not required if your environment is on ADD
azc = azfs.AzFileClient()

# credential is required if your environment is not on ADD
credential = "[your storage account credential]"
# or
credential = DefaultAzureCredential()
azc = azfs.AzFileClient(credential=credential)

```

#### types of authorization

Currently, only support [Azure Active Directory (ADD) token credential](https://docs.microsoft.com/azure/storage/common/storage-auth-aad).


### download data

azfs can get csv or json data from blob storage.

```python
import azfs
import pandas as pd

azc = azfs.AzFileClient()

# read csv as pd.DataFrame
df = azc.read_csv("https://[storage-account].../*.csv")
# or
with azc:
    df = pd.read_csv_az("https://[storage-account].../*.csv")

data = azc.read_json("https://[storage-account].../*.json")
```

### upload data

```python
import azfs
import pandas as pd

azc = azfs.AzFileClient()

df = pd.DataFrame()
data = {"example": "data"}

# write csv
azc.write_csv(path="https://[storage-account].../*.csv", df=df)
# or
with azc:
    df.to_csv_az(path="https://[storage-account].../*.csv", index=False)

# read json as dict
azc.write_json(path="https://[storage-account].../*.json", data=data)
```

### enumerating or checking if file exists

```python
import azfs

azc = azfs.AzFileClient()

# get file list of blob
file_list = azc.ls("https://[storage-account].../")

# check if file exists
is_exists = azc.exists("https://[storage-account].../*.csv")
```

### remove, copy files

```python
import azfs

azc = azfs.AzFileClient()

# copy file from `src_path` to `dst_path`
src_path = "https://[storage-account].../from/*.csv"
dst_path = "https://[storage-account].../to/*.csv"
is_copied = azc.cp(src_path=src_path, dst_path=dst_path, overwrite=True)

# remove the file
is_removed = azc.rm(path=src_path)
```


## dependencies

```
pandas >= "1.0.0"
azure-identity >= "1.3.1"
azure-storage-file-datalake >= "12.0.0"
azure-storage-blob >= "12.3.0"
```

## references

* [azure-sdk-for-python/storage](https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/storage)
* [filesystem_spec](https://github.com/intake/filesystem_spec)