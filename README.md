# azfs

[![CircleCI](https://circleci.com/gh/gsy0911/azfs.svg?style=svg&circle-token=ccd8e1ece489b247bcaac84861ae725b0f89a605)](https://circleci.com/gh/gsy0911/azfs)

AzFS is to provide convenient Python read/write functions for Azure Storage Account.

azfs can

* list files is blob,
* check if file exists,
* read csv as pd.DataFrame, and json as dict from blob,
* write pd.DataFrame as csv, and dict as json to blob,
* and raise lots of exceptions ! (Thank you for your cooperation)

## install

```bash
pip install git+https://github.com/gsy0911/azfs.git#egg=azfs
```

## usage

```python
import azfs
import pandas as pd


credential = "[your storage account credential]"
azc = azfs.AzFileClient(credential=credential)

# get file list of blob
file_list = azc.ls("https://[storage-account].../")

# check if file exists
is_exists = azc.exists("https://[storage-account].../*.csv")

# read csv as pd.DataFrame
df = azc.read_csv("https://[storage-account].../*.csv")
# or
with azc:
    df2 = pd.read_csv_az("https://[storage-account].../*.csv")

# write csv
azc.write_csv(path="https://[storage-account].../*.csv", df=df)
# or
with azc:
    df2.to_csv_az(path="https://[storage-account].../*.csv", index=False)

# read json as dict
data = azc.read_json("https://[storage-account].../*.json")
azc.write_json(path="https://[storage-account].../*.json", data=data)
```

## dependencies

```
pandas = "^1.0.3"
azure-identity = "^1.3.1"
azure-storage-file-datalake = "^12.0.0"
azure-storage-blob = "^12.3.0"
```

## references

* [azure-sdk-for-python/storage](https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/storage)
* [filesystem_spec](https://github.com/intake/filesystem_spec)