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

credential = "[your storage account credential]"
# or
credential = DefaultAzureCredential()

azc = azfs.AzFileClient(credential=credential)
```

#### types of authorization

Currently, only support [Azure Active Directory (ADD) token credential](https://docs.microsoft.com/azure/storage/common/storage-auth-aad).


### download data

azfs can get csv or json data from blob storage.

```
import azfs
import pandas as pd

credential = "[your storage account credential]"
azc = azfs.AzFileClient(credential=credential)

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

credential = "[your storage account credential]"
azc = azfs.AzFileClient(credential=credential)

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

credential = "[your storage account credential]"
azc = azfs.AzFileClient(credential=credential)

# get file list of blob
file_list = azc.ls("https://[storage-account].../")

# check if file exists
is_exists = azc.exists("https://[storage-account].../*.csv")
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