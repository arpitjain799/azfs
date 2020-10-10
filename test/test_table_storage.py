from azure.cosmosdb.table.tableservice import TableService

from azfs import TableStorage, TableStorageWrapper


TABLE_NAME = "test_table"
PARTITION_KEY = "test_partition"
ROW_KEY = "test_row_key"
cons = {
    "account_name": "test",
    "account_key": "abcdefg1234",
    "database_name": TABLE_NAME,
}

table_storage = TableStorage(**cons)


def test_table_storage_put(mocker):
    # data
    put_data = {"test_key": "test_value"}

    # mock
    func_mock = mocker.MagicMock()

    # patch
    mocker.patch.object(TableService, "insert_entity", func_mock)
    _ = table_storage.put(partition_key_value=PARTITION_KEY, data=put_data)


def test_table_storage_get(mocker):
    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = []

    # patch
    mocker.patch.object(TableService, "query_entities", func_mock)
    _ = table_storage.get(partition_key_value=PARTITION_KEY, filter_key_values={})

    # filter value
    filter_value = f"PartitionKey eq '{PARTITION_KEY}'"
    func_mock.assert_called_with(table_name=TABLE_NAME, filter=filter_value)


def test_table_storage_update(mocker):
    #
    put_data = {"test_key": "test_value_2"}

    # mock
    func_mock = mocker.MagicMock()
    func_mock.return_value = []

    # patch
    mocker.patch.object(TableService, "update_entity", func_mock)
    _ = table_storage.update(partition_key_value=PARTITION_KEY, row_key=ROW_KEY, data=put_data)

    # filter value
    updated_data = {'PartitionKey': PARTITION_KEY, 'RowKey': ROW_KEY}
    updated_data.update(put_data)
    func_mock.assert_called_with(table_name=TABLE_NAME, entity=updated_data)

