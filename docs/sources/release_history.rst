Release History
===============

0.2.4 (2020-10-10)
------------------

* add ``TableStorage``, and ``TableStorageWrapper`` class to manipulate TableStorage.

0.2.3 (2020-09-09)
------------------

* Modify ``AzfsInputError`` message from Japanese to English.

0.2.2 (2020-08-26)
------------------

* partially implement ``AzFileSystem``: at least it works.
* add type hinting
* add ``read_parquet()``: to read parquet file in Azure.

0.2.1 (2020-07-19)
------------------

* partially adopt chunk-reading for csv file.
* implement ``AzfsFileClient::read_line_iter()``: to read text file in each line with iterator.
* implement ``BlobPathDecoder.add_pattern()``: to decode user-defined path

0.2.0 (2020-07-14)
------------------

* can upload over 100MB data to DataLakeStorage.
* support ``connection_string``: in ``azfs.AzFileClient()``

0.1.10 (2020-07-05)
-------------------

* implement new functions: read/write ``pickle`` and ``tsv`` format.
* implement ``AzContextManager()``: set and get attributes to pandas easily
* code optimization ``glob()``: make execution faster, and add limitation (not allowed to glob() in root folder under a container)

0.1.9 (2020-06-30)
------------------

* implement ``_get_service_client()``: to prepare adopting new authorization
* code optimization: ``BlobPathDecoder`` class

0.1.8 (2020-06-22)
------------------

* modify ``write_csv()``: apply default encoding is ``UTF-8``
* modify ``exists()``

0.1.7 (2020-06-07)
------------------

* modify ``ls()``:
* modify ``put()`` and ``get()`` in Queue: apply Base64 encode/decode

0.1.6 (2020-06-02)
------------------

* check compatibility on ``Python 3.6`` and ``Python 3.8``
* modify ``glob()``: compile regex

0.1.5 (2020-05-30)
------------------

* implementing ``glob()``
* add ``prefix``-parameter to ``ls()``
* add ``Queue`` operation class

0.1.4 (2020-05-14)
------------------

* add ``**kwargs`` to read/write functions

0.1.3 (2020-05-12)
------------------

* add implementing candidate methods
* modify ``ls()``: add filter

0.1.2 (2020-05-10)
------------------

* remove if-statement using metaclass
* add test on ``PyTest``

0.1.1 (2020-05-03)
------------------

* add ``DataLakeClient``

0.1.0 (2020-04-29)
------------------

* initial release

