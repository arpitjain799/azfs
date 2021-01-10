###############
Release History
###############

0.2.14 (2021-01-11)
*******************

* add default logger
    * ``logger = getLogger("azfs")`` (`#144 <https://github.com/gsy0911/azfs/issues/144>`_)
* update ``azfs.ExportDecorator()``
    * ignore exception if ``ignore_error`` is True (`#142 <https://github.com/gsy0911/azfs/issues/142>`_)

0.2.13 (2020-12-22)
*******************

* update ``azfs.ExportDecorator()``
    * accept ``default_parameter`` each argument (`#128 <https://github.com/gsy0911/azfs/issues/128>`_)
* BUG FIX: cli command ``$ azfs decorator -n {file_name}``
    * NameError occurred (`#132 <https://github.com/gsy0911/azfs/issues/132>`_)

0.2.12 (2020-12-15)
*******************

* update ``azfs.ExportDecorator()``
    * attach ``under_bar`` each argument to avoid conflict (`#130 <https://github.com/gsy0911/azfs/issues/130>`_)

0.2.11 (2020-12-13)
*******************

* update ``azfs.ExportDecorator()``
    * accept basic argument (`#123 <https://github.com/gsy0911/azfs/issues/123>`_)
    * add appropriate error (`#124 <https://github.com/gsy0911/azfs/issues/124>`_)
* add cli command ``$ azfs decorator -n {file_name}``
    * to avoid PEP8 violation on PyCharm(`#121 <https://github.com/gsy0911/azfs/issues/121>`_)

0.2.10 (2020-12-10)
*******************

* update ``azfs.ExportDecorator()`` (`#120 <https://github.com/gsy0911/azfs/issues/120>`_)
    * accept multiple ``file_name`` and multiple return values from ``user-defined function``
    * accept ``str`` or ``dict``, when ``import_decorator()``


0.2.9 (2020-12-08)
******************

* add Experimental feature: ``azfs.ExportDecorator()``

0.2.8 (2020-11-24)
******************

* BUG FIX: ``azc.read(use_mp=True)`` is not working correctly, if credential is AzureDefaultCredentials.
* BUG FIX: ``glob(path)`` is not working correctly if path contains special characters like ``(`` or ``)``, etc.

0.2.7 (2020-11-13)
******************

* BUG FIX: ``write_json(ensure_ascii=False)`` is not working correctly if data-json has non-ascii character.

0.2.6 (2020-11-11)
******************

* add multiprocessing-read, as ``azc.read(use_mp=True).csv()``.
* add apply() function, as ``azc.read(use_mp=True).apply(function=some_function).csv()``
* modify glob(): not working under a certain directory.

0.2.5 (2020-11-01)
******************

* modify exists(): use ``info()`` instead of ``_get()``.
* add pyspark-like read method, such as ``azc.read().csv()``, ``azc.read().parquet()``.
* add ``__all__``, and organize the directory.

0.2.4 (2020-10-10)
******************

* add ``TableStorage``, and ``TableStorageWrapper`` class to manipulate TableStorage.

0.2.3 (2020-09-09)
******************

* Modify ``AzfsInputError`` message from Japanese to English.

0.2.2 (2020-08-26)
******************

* partially implement ``AzFileSystem``: at least it works.
* add type hinting
* add ``read_parquet()``: to read parquet file in Azure.

0.2.1 (2020-07-19)
******************

* partially adopt chunk-reading for csv file.
* implement ``AzfsFileClient::read_line_iter()``: to read text file in each line with iterator.
* implement ``BlobPathDecoder.add_pattern()``: to decode user-defined path

0.2.0 (2020-07-14)
******************

* can upload over 100MB data to DataLakeStorage.
* support ``connection_string``: in ``azfs.AzFileClient()``

0.1.10 (2020-07-05)
*******************

* implement new functions: read/write ``pickle`` and ``tsv`` format.
* implement ``AzContextManager()``: set and get attributes to pandas easily
* code optimization ``glob()``: make execution faster, and add limitation (not allowed to glob() in root folder under a container)

0.1.9 (2020-06-30)
******************

* implement ``_get_service_client()``: to prepare adopting new authorization
* code optimization: ``BlobPathDecoder`` class

0.1.8 (2020-06-22)
******************

* modify ``write_csv()``: apply default encoding is ``UTF-8``
* modify ``exists()``

0.1.7 (2020-06-07)
******************

* modify ``ls()``:
* modify ``put()`` and ``get()`` in Queue: apply Base64 encode/decode

0.1.6 (2020-06-02)
******************

* check compatibility on ``Python 3.6`` and ``Python 3.8``
* modify ``glob()``: compile regex

0.1.5 (2020-05-30)
******************

* implementing ``glob()``
* add ``prefix``-parameter to ``ls()``
* add ``Queue`` operation class

0.1.4 (2020-05-14)
******************

* add ``**kwargs`` to read/write functions

0.1.3 (2020-05-12)
******************

* add implementing candidate methods
* modify ``ls()``: add filter

0.1.2 (2020-05-10)
******************

* remove if-statement using metaclass
* add test on ``PyTest``

0.1.1 (2020-05-03)
******************

* add ``DataLakeClient``

0.1.0 (2020-04-29)
******************

* initial release

