# azfs

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