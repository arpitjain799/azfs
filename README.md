# azfs

AzFS is to provide convenient Python read/write functions for Azure Storage Account.


## install

```bash
pip install git+https://github.com/gsy0911/azfs.git#egg=azfs
```

## usage

```python
import azfs
credential = '[your storage account credential]'
azc = azfs.AzFileClient(credential=credential)

# read csv as pd.DataFrame
df = azc.read_csv("https://[storage-account].../*.csv")

# read json as dict
data = azc.read_json("https://[storage-account].../*.json")
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