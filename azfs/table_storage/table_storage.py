import functools
from inspect import signature
from typing import List, Dict

from azfs.error import AzfsInputError

from azure.cosmosdb.table.tableservice import TableService


class TableStorage:
    """
    A class for manipulating TableStorage in Storage Account
    The class provides simple methods below.

    * create
    * read
    * update
    * delete(not yet)

    The class is intended to be used as `delegation`, not `extend`.

    Args:
        account_name: name of the Storage Account
        account_key: key for the Storage Account
        database_name: name of the StorageTable database

    """

    def __init__(self, account_name: str, account_key: str, database_name: str):
        """

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

        tasks = self.table_service.query_entities(table_name=self.database_name, filter=" and ".join(filter_value_list))
        return [task for task in tasks]

    def put(self, partition_key_value: str, data: dict):
        """
        put data.

        Args:
            partition_key_value:
            data:

        Returns:

        """
        insert_data = {'PartitionKey': partition_key_value}
        insert_data.update(data)
        self.table_service.insert_entity(table_name=self.database_name, entity=insert_data)
        return insert_data

    def update(self, partition_key_value: str, row_key: str, data: dict):
        """
        update data.

        Args:
            partition_key_value:
            row_key:
            data:

        Returns:

        """
        updated_data = {'PartitionKey': partition_key_value, 'RowKey': row_key}
        updated_data.update(data)
        self.table_service.update_entity(table_name=self.database_name, entity=updated_data)
        return updated_data


class TableStorageWrapper:
    """
    Wrapper for the TableStorage class.

    Args:
        account_name: name of the Storage Account
        account_key: key for the Storage Account
        database_name: name of the StorageTable database
        partition_key:
        row_key_name:

    Examples:
        >>> import json
        >>> from datetime import datetime
        >>> from pytz import timezone
        >>> tokyo = timezone('Asia/Tokyo')
        >>> cons = {
        ...     "account_name": "{storage_account_name}",
        ...     "account_key": "{credential}",
        ...     "database_name": "{database_name}"
        ... }
        # you can manipulate data through `simple_table_client`
        >>> simple_table_client = TableStorageWrapper(partition_key="simple_table", **cons)
        # store data according to the keyword-arguemnt you put
        # by default, `id_` is converted to `RowKey`, then `id_` is not stored
        >>> simple_table_client.put(id_="1", message="hello_world")
        ... {'PartitionKey': 'simple_table', 'message': 'hello_world', 'RowKey': '1'}
        # can get all data, simply call
        >>> simple_table_client.get()
        ... ...
        # or filter with specific value, like
        # `id_` is configured as `RowKey` by default
        >>> simple_table_client.get(id_="1")
        ... [
        ...     {
        ...         'PartitionKey': 'simple_table',
        ...         'RowKey': '1',
        ...         'Timestamp': datetime.datetime(2020, 10, 10, 3, 15, 57, 874427, tzinfo=tzutc()),
        ...         'message': 'hello_world',
        ...         'etag': 'W/"datetime\'2020-10-10T03%3A15%3A57.8744271Z\'"'
        ...     }
        ... ]
        # In addition, you can store data in different way
        >>> complex_client = TableStorageWrapper(partition_key="complex_table", **cons)
        >>> @complex_client.overwrite_pack_data_to_put()
        ... def modify_put_data(id_: str, message: str):
        ...     alt_message = json.dumps({datetime.now(tz=tokyo).isoformat(): message}, ensure_ascii=False)
        ...     return {"id_": id_, "message": alt_message}
        # you can store data in a different way
        >>> complex_client.put(id_="2", message="hello_world")
        ... {
        ...     'PartitionKey': 'complex_table',
        ...     'message': '{"2020-10-10T12:26:57.442718+09:00": "hello_world"}',
        ...     'RowKey': '2'
        ... }
        # you can also modify update function, with restriction example
        >>> @complex_client.overwrite_pack_data_to_update(allowed={"message": ["ERROR", "RUNNING", "SUCCESS"]})
        ... def modify_update_data(id_: str, message: str):
        ...     d = complex_client.get(id_=id_)
        ...     message_dict = json.loads(d[0]['message'])
        ...     if type(message_dict) is dict:
        ...         message_dict[datetime.now(tz=tokyo).isoformat()] = message
        ...     else:
        ...         message_dict = {datetime.now(tz=tokyo).isoformat(): message}
        ...
        ...     data = {
        ...         "id_": id_,
        ...         "message": json.dumps(message_dict, ensure_ascii=False)
        ...     }
        ...     return data
        >>> complex_client.update(id_="2", message="RUNNING")
        ... {
        ...     'PartitionKey': 'complex_table',
        ...     'RowKey': '2',
        ...     'message': '{"2020-10-10T12:26:57.442718+09:00": "hello_world", "2020-10-10T13:00:23.602943+09:00": "RUNNING"}'
        ... }
    """
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

    @staticmethod
    def _check_argument(function: callable, arg_name: str) -> None:
        """
        Check whether the `arg_name` in the parameter of the `function`.

        Args:
            function: function to check
            arg_name: argument name

        Raises:
            ArgumentNameInvalidError: When `arg_name` is not found in the parameter of the `function`

        """
        # check arguments
        sig = signature(function)
        if arg_name not in sig.parameters:
            raise AzfsInputError(f"{arg_name} not in {function.__name__}")

    def _overwrite_pack_data(self, _to: str, allowed: Dict[str, List[str]] = None):
        """

        Args:
            _to:
            allowed:

        Returns:

        """
        def _wrapper(function: callable):
            # check if `row_key` argument exists
            self._check_argument(function=function, arg_name=self.row_key_name)

            def _actual_function(*args, **kwargs):
                # check if the value exists on the kwargs
                if allowed:
                    for k, v in allowed.items():
                        if kwargs[k] not in v:
                            raise AzfsInputError(f"keyword argument {kwargs[k]} is not allowed in {k}. {v} are allowed")
                return function(*args, **kwargs)

            # overwrite the attribute
            setattr(self, _to, _actual_function)

            return function
        return _wrapper

    # =======
    # get
    # =======

    def get(self, **kwargs) -> List[dict]:
        """
        get data.

        Args:
            **kwargs:

        Returns:

        """
        if self.row_key_name in kwargs:
            kwargs['RowKey'] = kwargs.pop(self.row_key_name)
        return self.st.get(partition_key_value=self.partition_key, filter_key_values=kwargs)

    # =======
    # put
    # =======

    @staticmethod
    def pack_data_to_put(**kwargs):
        """

        Args:
            **kwargs:

        Returns:

        """
        return kwargs

    def put(self, **kwargs):
        """

        Args:
            **kwargs:

        Returns:

        """
        _data = self.pack_data_to_put(**kwargs)
        if self.row_key_name in _data:
            _data['RowKey'] = _data.pop(self.row_key_name)
        return self.st.put(partition_key_value=self.partition_key, data=_data)

    overwrite_pack_data_to_put = functools.partialmethod(_overwrite_pack_data, _to="pack_data_to_put")

    # =======
    # update
    # =======

    @staticmethod
    def pack_data_to_update(**kwargs):
        """

        Args:
            **kwargs:

        Returns:

        """
        return kwargs

    def update(self, **kwargs):
        """

        Args:
            **kwargs:

        Returns:

        """
        _data = self.pack_data_to_update(**kwargs)
        row_key = _data.pop(self.row_key_name)
        return self.st.update(partition_key_value=self.partition_key, row_key=row_key, data=_data)

    overwrite_pack_data_to_update = functools.partialmethod(_overwrite_pack_data, _to="pack_data_to_update")
