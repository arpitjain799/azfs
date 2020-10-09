from typing import List
from azure.cosmosdb.table.tableservice import TableService


class TableStorage:
    """
    A class for manipulating TableStorage in Storage Account
    The class provides simple methods below.
    * create
    * read
    * update
    * delete(not yet)
    """

    def __init__(self, account_name: str, account_key: str, database_name: str):
        """

        Args:
            account_name: name of the Storage Account
            account_key: key for the Storage Account
            database_name: name of the StorageTable database
        """
        self.table_service = TableService(account_name=account_name, account_key=account_key)
        self.database_name = database_name

    def get(self, partition_key_value: str, filter_key_values: dict):
        """
        use PartitionKey as table_name

        Args:
            partition_key_value:
            filter_key_values:

        Returns:

        """
        filter_value_list = [
            f"PartitionKey eq '{partition_key_value}'"
        ]

        # その他の条件を付与する場合
        filter_value_list.extend(
            [f"{k} eq '{v}'" for k, v in filter_key_values.items()]
        )

        tasks = self.table_service.query_entities(
            self.database_name, filter=" and ".join(filter_value_list))
        return [task for task in tasks]

    def put(self, partition_key_value: str, data: dict):
        insert_data = {'PartitionKey': partition_key_value}
        insert_data.update(data)
        self.table_service.insert_entity(self.database_name, insert_data)
        return insert_data

    def update(self, partition_key_value: str, row_key: str, data: dict):
        updated_data = {'PartitionKey': partition_key_value, 'RowKey': row_key}
        updated_data.update(data)
        self.table_service.update_entity(self.database_name, updated_data)
        return updated_data


class TableStorageWrapper:
    def __init__(
            self,
            account_name,
            account_key,
            database_name,
            partition_key: str,
            row_key_name: str = "id_"):
        self.st = TableStorage(account_name=account_name, account_key=account_key, database_name=database_name)
        self.partition_key = partition_key
        self.row_key_name = row_key_name

    def get(self, **kwargs) -> List[dict]:
        if self.row_key_name in kwargs:
            kwargs['RowKey'] = kwargs.pop(self.row_key_name)
        return self.st.get(partition_key_value=self.partition_key, filter_key_values=kwargs)

    def put(self, **kwargs):
        data = self.pack_data_to_put(**kwargs)
        return self.st.put(partition_key_value=self.partition_key, data=data)

    def pack_data_to_put(self, **kwargs):
        if self.row_key_name in kwargs:
            kwargs['RowKey'] = kwargs.pop(self.row_key_name)
        return kwargs

    def update(self, **kwargs):
        row_key = kwargs.pop(self.row_key_name)
        return self.st.update(partition_key_value=self.partition_key, row_key=row_key, data=kwargs)
