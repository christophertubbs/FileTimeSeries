"""
Handles database operations that may be coming from multiple threads
"""
import dataclasses
import os
import queue
import typing
import logging
import pathlib
import sqlite3
import threading

import pandas

LOGGER: logging.Logger = logging.getLogger(pathlib.Path(__file__).stem)

_LOCK: threading.RLock = threading.RLock()

@dataclasses.dataclass
class WriteRequest:
    path: pathlib.Path
    table_name: str
    data: pandas.DataFrame

def listen_and_write(
    stop_signal: threading.Event,
    write_queue: queue.Queue[WriteRequest],
    timeout_seconds: float = 1.0
) -> int:
    """
    Poll a write queue as long as a stop signal has not been set and
    write everything that comes through to a database table

    :param stop_signal: A signal telling the loop to stop
    :param write_queue: A queue that write requests will be sent to
    :param timeout_seconds: How long to wait for an entry before checking if it should keep polling
    :return: The number of entries written
    """
    write_count: int = 0
    while not stop_signal.is_set():
        try:
            request: WriteRequest = write_queue.get(
                block=True,
                timeout=timeout_seconds
            )
        except queue.Empty:
            continue

        if not isinstance(request, WriteRequest):
            LOGGER.error(f"Received an invalid write request: {request}")
            continue

        try:
            write_to_db(path=request.path, table_name=request.table_name, data=request.data)
            write_count += len(request.data)
        except Exception as e:
            LOGGER.exception("Failed to write to database", exc_info=e, stack_info=True)
    return write_count

def write_to_db(
    path: pathlib.Path,
    table_name: str,
    data: pandas.DataFrame
):
    """
    Add data from a dataframe to database

    :param path: The path to a sqlite database
    :param table_name: The name of the table to add to
    :param data: The data to add
    """
    with _LOCK:
        LOGGER.debug(f"Looking to add data to {path}")

        add_table(path=path, table_name=table_name, data=data)
        column_names: typing.List[str] = list(data.index.names) + list(data.columns)
        insert_sql: str = f"""INSERT OR IGNORE INTO "{table_name}" (
    {(", " + os.linesep + "    ").join(column_names)}
) VALUES (
    {(", " + os.linesep + "    ").join(['?' for _ in column_names])}
)
"""
        unindexed_table: pandas.DataFrame = data.reset_index(drop=False)
        rows: typing.List[typing.Tuple] = [
            tuple([value for value in entries])
            for entries in unindexed_table.values
        ]

        try:
            connection: sqlite3.Connection = sqlite3.connect(path)
            cursor: sqlite3.Cursor = connection.cursor()
            cursor.executemany(insert_sql, rows)
            connection.commit()
        finally:
            if connection:
                try:
                    connection.close()
                except:
                    logging.exception("Failed to close connection", exc_info=True)


def add_table(
    path: pathlib.Path,
    table_name: str,
    data: pandas.DataFrame,
):
    """
    Add a table to a SQLite database at the given path if it does not exist.

    The schema from the given data is used. The data must be indexed to define a pk.
    When new values are given, matching pks will be ignored

    :param path: The path to a sqlite database
    :param table_name: The name of the table to add
    :param data: The dataframe that describes the schema
    """
    if not data:
        raise ValueError(f"Cannot add the '{table_name}' table - there is no data to base it off of")

    if data.index.name is None and data.index.names is None:
        raise ValueError(f"The dataframe to base the table off of must have a named index/indices and it does not")

    with _LOCK:
        connection: typing.Optional[sqlite3.Connection] = None
        try:
            connection = sqlite3.connect(path)
            if table_exists(connection=connection, table_name=table_name):
                return

            column_definitions: typing.List[str] = []

            if data.index.names:
                for column_name in data.index.names:
                    column_definitions.append(
                        f'"{column_name}" {pandas_dtype_to_sqlite_type(data.index.dtypes[column_name])}'
                    )
            else:
                column_definitions.append(
                    f'{data.index.name} {pandas_dtype_to_sqlite_type(data.index.dtype)} PRIMARY KEY')


            for column in data.columns:
                column_definitions.append(
                    f'"{column}" {pandas_dtype_to_sqlite_type(data.dtypes[column])}'
                )

            if data.index.names:
                column_definitions.append(
                    f'PRIMARY KEY ({", ".join(data.index.names)})'
                )

            column_definition_sql: str = f",{os.linesep}    ".join(column_definitions)

            table_creation_sql: str = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    {column_definition_sql}
)"""
            cursor: sqlite3.Cursor = connection.cursor()
            cursor.execute(table_creation_sql)
            connection.commit()
        finally:
            if connection:
                try:
                    connection.close()
                except:
                    LOGGER.exception("Failed to close database connection")


def table_exists(
    connection: sqlite3.Connection,
    table_name: str
) -> bool:
    """
    Checks if a table exists in the given database

    :param connection: A connection to the database
    :param table_name: The name of the table to look for
    :return: True if the table exists, False otherwise
    """
    cursor = connection.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cursor.fetchone() is not None


def pandas_dtype_to_sqlite_type(dtype: pandas.api.extensions.ExtensionDtype) -> str:
    """
    Get the matching sqlite datatype from the given pandas dtype

    :param dtype: The type of column from a pandas data frame
    :return: The name of the sqlite datatype
    """
    if pandas.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pandas.api.types.is_bool_dtype(dtype):
        return "INTEGER"
    if pandas.api.types.is_float_dtype(dtype):
        return "REAL"
    return "TEXT"