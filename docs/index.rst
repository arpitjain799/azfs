.. azfs documentation master file, created by
   sphinx-quickstart on Sat Jun 27 16:18:45 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

AzFS
====

code status

.. image:: https://circleci.com/gh/gsy0911/azfs.svg?style=svg&circle-token=ccd8e1ece489b247bcaac84861ae725b0f89a605
    :target: https://circleci.com/gh/gsy0911/azfs

.. image:: https://codecov.io/gh/gsy0911/azfs/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/gsy0911/azfs

.. image:: https://img.shields.io/lgtm/grade/python/g/gsy0911/azfs.svg?logo=lgtm&logoWidth=18
    :target: https://lgtm.com/projects/g/gsy0911/azfs/context:python

.. image:: https://readthedocs.org/projects/azfs/badge/?version=latest
   :target: https://azfs.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

package status

.. image:: https://img.shields.io/badge/python-3.6|3.7|3.8-blue.svg
   :target: https://www.python.org/downloads/release/python-377/

.. image:: https://img.shields.io/pypi/v/azfs.svg
    :target: https://pypi.org/project/azfs/

.. image:: https://pepy.tech/badge/azfs
   :target: https://pepy.tech/project/azfs

**AzFS** is to provide convenient Python read/write functions for Azure Storage Account.

``AzFS`` can

* list files in blob (also filtered with wildcard ``*``),
* check if file exists,
* read csv as pd.DataFrame, and json as dict from blob,
* write pd.DataFrame as csv, and dict as json to blob,
* and raise lots of exceptions ! (Thank you for your cooperation)

Installation
============

**AzFS** can be installed from pip.

.. code-block:: shell

   pip install azfs

Limitation
==========

authorization
-------------

Supported authentication types are

* `Azure Active Directory (AAD) token credential <https://docs.microsoft.com/azure/storage/common/storage-auth-aad>`_
* connection_string, like `DefaultEndpointsProtocol=https;AccountName=xxxx;AccountKey=xxxx;EndpointSuffix=core.windows.net`

types of storage
----------------

The table below shows the compatibility read/access of ``AzFS``.

+--------------+------+-----------+-------+------+-------+
| account kind | Blob | Data Lake | Queue | File | Table |
+==============+======+===========+=======+======+=======+
| StorageV2    | O    | O         | O     | X    | X     |
+--------------+------+-----------+-------+------+-------+
| StorageV1    | O    | O         | O     | X    | X     |
+--------------+------+-----------+-------+------+-------+
| BlobStorage  | O    |           |       |      |       |
+--------------+------+-----------+-------+------+-------+


API Usage
=========


create the client
-----------------

To manipulate files in Azure Blob Storage,
firstly you need to create `AzFileClient <./sources/api.html#azfileclient>`_.

Credential is not required if ``AzFileClient()`` is created on AAD (Azure Active Directory).


.. code-block:: python

   import azfs
   from azure.identity import DefaultAzureCredential

   # credential is not required if your environment is on AAD
   azc = azfs.AzFileClient()
   # credential is required if your environment is not on AAD
   credential = "[your storage account credential]"
   # or
   credential = DefaultAzureCredential()
   azc = azfs.AzFileClient(credential=credential)

   # connection_string is also supported
   connection_string = "DefaultEndpointsProtocol=https;AccountName=xxxx;AccountKey=xxxx;EndpointSuffix=core.windows.net"
   azc = azfs.AzFileClient(connection_string=connection_string)


download data
-------------

``AzFS`` provides function to download csv or json data from ``Azure Blob Storage``.
API reference is `get/download <./sources/api.html#get-download>`_.

.. code-block:: python

   import azfs
   import pandas as pd

   azc = azfs.AzFileClient()
   # if your data in BlobStorage
   csv_path = "https://{storage_account}.blob.core.windows.net/{container}/***.csv"
   json_path = "https://{storage_account}.blob.core.windows.net/{container}/***.json"
   data_path = "https://{storage_account}.blob.core.windows.net/{container}/***.another_format"
   # if your data in DataLakeStorage
   csv_path = "https://{storage_account}.dfs.core.windows.net/{container}/***.csv"
   json_path = "https://{storage_account}.dfs.core.windows.net/{container}/***.json"
   data_path = "https://{storage_account}.dfs.core.windows.net/{container}/***.another_format"

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


upload data
-----------

``AzFS`` also provides functions to upload csv or json data to ``Azure Blob Storage``.
API reference is `put/upload <./sources/api.html#put-upload>`_.


.. code-block:: python

   import azfs
   import pandas as pd

   azc = azfs.AzFileClient()
   # if your data in BlobStorage
   csv_path = "https://{storage_account}.blob.core.windows.net/{container}/***.csv"
   json_path = "https://{storage_account}.blob.core.windows.net/{container}/***.json"
   data_path = "https://{storage_account}.blob.core.windows.net/{container}/***..another_format"


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


enumerating(ls, glob) or checking if file exists
------------------------------------------------

``ls()`` lists all files in specified folder,
and ``glob()`` lists pattern-matched files in all folder.
API reference is `enumerating <./sources/api.html#file-enumerating>`_.

.. code-block:: python

   import azfs

   azc = azfs.AzFileClient()

   # get file_name list of blob
   file_name_list = azc.ls("https://{storage_account}.blob.core.windows.net/{container}")
   # or if set `attach_prefix` True, get full_path list of blob
   file_full_path_list = azc.ls("https://{storage_account}.blob.core.windows.net/{container}", attach_prefix=True)

   # find specific file with `*`
   file_full_path_list = azc.glob("https://{storage_account}.blob.core.windows.net/{container}/some_folder/*.csv")
   # also search deeper directory
   file_full_path_list = azc.glob("https://{storage_account}.blob.core.windows.net/{container}/some_folder/*/*.csv")
   # or if the directory starts with `a`
   file_full_path_list = azc.glob("https://{storage_account}.blob.core.windows.net/{container}/some_folder/a*/*.csv")

   # check if file exists
   is_exists = azc.exists("https://{storage_account}.blob.core.windows.net/{container}/some_folder/test.csv")


remove, copy files, etc...
--------------------------

``AzFS`` also provides remove and copy functions.
API reference is `manipulating <./sources/api.html#file-manipulating>`_.

.. code-block:: python

   import azfs

   azc = azfs.AzFileClient()

   # copy file from `src_path` to `dst_path`
   src_path = "https://{storage_account}.blob.core.windows.net/{container}/src_folder/*.csv"
   dst_path = "https://{storage_account}.blob.core.windows.net/{container}/dst_folder/*.csv"
   is_copied = azc.cp(src_path=src_path, dst_path=dst_path, overwrite=True)

   # remove the file
   is_removed = azc.rm(path=src_path)

   # get file meta info
   data = azc.info(path=src_path)



For Users
=========

.. toctree::
   :maxdepth: 2

   sources/api
   sources/release_history

For Developers
==============

.. toctree::
   :maxdepth: 2

   sources/development_api

GitHub
------

**AzFS** repository is `here <https://github.com/gsy0911/azfs>`_.

References
----------

* `azure-sdk-for-python/storage <https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/storage>`_
* `filesystem_spec <https://github.com/intake/filesystem_spec>`_

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`
