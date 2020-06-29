Release History
===============

.. toctree::
   :maxdepth: 4


0.1.9 ()
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

