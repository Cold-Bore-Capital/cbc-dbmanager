import time
from datetime import datetime
from typing import List, Any, Dict, Tuple, Union

import numpy as np
import pandas as pd
from numpy import inf
from psycopg2 import connect
from psycopg2.extras import execute_values, execute_batch
from sshtunnel import open_tunnel, create_logger

from cbcdb.configuration_service import ConfigurationService as Config


class DBManager:
    """
    DBManager handled the read and write information to the DB.
    This class handles the transport of data to and from the db. The goal is to use
    highly reusable methods, shareable across most of the classes.
    """

    def __init__(self,
                 debug_output_mode=None,
                 use_ssh=False,
                 ssh_key_path=None,
                 ssh_host=None,
                 ssh_port=None,
                 ssh_user=None,
                 ssh_remote_bind_address=None,
                 ssh_remote_bind_port=None,
                 ssh_local_bind_address=None,
                 ssh_local_bind_port=None,
                 db_name=None,
                 db_user=None,
                 db_password=None,
                 db_schema=None,
                 db_host=None):
        """
        Init Function

        Args:
            debug_output_mode: Flag to turn on debug mode. Setting this to True will print debug messages.
            use_ssh: A flag to indicate if SSH should be used for the connection. If set to True, database connection
                     will be made through an SSH tunnel.
            ssh_key_path: A path to the SSH key on the local computer or container disk.
            ssh_host: The host for the SSH tunnel.
            ssh_port:
            ssh_user:
            ssh_remote_bind_address:
            ssh_remote_bind_port:
            ssh_local_bind_address:
            ssh_local_bind_port: In the .env file you can either set an integer, or use the word "random" to allow the
                                 system to select a raondom port.
            db_name:
            db_user:
            db_password:
            db_schema:
            db_host:
        """
        self._config = Config(debug_output_mode=None,
                              use_ssh=None,
                              ssh_key_path=None,
                              ssh_host=None,
                              ssh_port=None,
                              ssh_user=None,
                              ssh_remote_bind_address=None,
                              ssh_remote_bind_port=None,
                              ssh_local_bind_address=None,
                              ssh_local_bind_port=None,
                              db_name=None,
                              db_user=None,
                              db_password=None,
                              db_schema=None,
                              db_host=None)
        self._debug_mode = self._config.debug_output_mode if not debug_output_mode else debug_output_mode
        self._db_host = db_host if db_host else self._config.db_host
        self._db_name = db_name if db_name else self._config.db_name
        self._db_user = db_user if db_user else self._config.db_user
        self._db_password = db_password if db_password else self._config.db_password
        # @todo Implement this as a default schema.
        self._db_schema = db_schema if db_schema else self._config.db_schema
        # Pubilicly accessable schema
        self._db_schema = self._db_schema
        self._db_port = ssh_remote_bind_port if ssh_remote_bind_port else self._config.db_port

        self.use_ssh = use_ssh if use_ssh else self._config.use_ssh
        if self.use_ssh:
            self.ssh_host = ssh_host if ssh_host else self._config.ssh_host
            self.ssh_port = ssh_port if ssh_port else self._config.ssh_port
            self.ssh_user = ssh_user if ssh_user else self._config.ssh_user
            self.ssh_key_path = ssh_key_path if ssh_key_path else self._config.ssh_key_path
            self.ssh_remote_bind_port = ssh_remote_bind_port if ssh_local_bind_port else self._config.db_port
            self.ssh_local_bind_address = ssh_local_bind_address if ssh_local_bind_address else self._config.ssh_local_bind_address
            self.ssh_local_bind_port = ssh_local_bind_port if ssh_local_bind_port else self._config.ssh_local_bind_port

        self._page_size = None

    def _print_debug_output(self, msg: str):
        """
        Prints a debug message if system is in debug mode.
        Args:
            msg: A string containing the message to print to the logs

        Returns: None

        """
        if self._debug_mode:
            print(f'DEBUG: {msg}')

    def _get_connection(self, sql, params, method_instance):
        """
        Creates a connection to the database
        Args:
            sql: The original SQL query.
            params: The original params.
            method_instance: An instance of the method requesting the database connection.

        Returns: The results of the original method instance.
        """
        if self.use_ssh:
            with open_tunnel(
                    (self.ssh_host, self.ssh_port),
                    ssh_username=self.ssh_user,
                    ssh_pkey=self.ssh_key_path,
                    remote_bind_address=(self._db_host, self.ssh_remote_bind_port),
                    local_bind_address=(self.ssh_local_bind_address, self.ssh_local_bind_port)) as tunnel:
                if self._config.ssh_logging_level:
                    tunnel.logger = create_logger(loglevel=self._config.ssh_logging_level)
                host = tunnel.local_bind_host
                port = tunnel.local_bind_port
                return self._database_connection_sub_method(host, port, method_instance, sql, params)

        else:
            host = self._db_host
            port = self._db_port
            return self._database_connection_sub_method(host, port, method_instance, sql, params)

    def _database_connection_sub_method(self, host, port, method_instance, sql, params):
        with connect(dbname=self._db_name,
                     host=host,
                     port=port,
                     user=self._db_user,
                     password=self._db_password) as conn:
            with conn.cursor() as curs:
                return method_instance(sql, params, curs, conn)

    def get_sql_dataframe(self, sql: str, params: list = None, curs=False, conn=False) -> pd.DataFrame:
        """
         Returns a DataFrame for a given SQL query

        Args:
            sql: SQL string
            params: List of parameters
            curs: An instance of a database cursor. Will be false when method is first called, then populated when
                  method is called recursively.
            conn: An instance of a database connection or false on first call.

        Returns: A Pandas DataFrame.

        """
        self._print_debug_output(f"Getting query:\n {sql}")

        if conn:
            if params:
                df = pd.read_sql_query(sql, params=params, con=conn)
            else:
                df = pd.read_sql_query(sql, con=conn)
        else:
            df = self._get_connection(sql, params, self.get_sql_dataframe)
        return df

    def get_sql_list_dicts(self, sql: str, params: list = None, curs=False, conn=False) -> List[Dict[str, Any]]:
        """
        Returns a list of dicts for a given SQL Query

        Args:
            sql: SQL string
            params: Parameters for SQL call
            curs: An instance of a database cursor. Will be false when method is first called, then populated when
                  method is called recursively.
            conn: An instance of a database connection or false on first call.


        Returns:
            Example Output:
            [{'some': 'data'},
             {'more': 'otherdata'}]

        """
        self._print_debug_output(f"Getting query:\n {sql}")
        if curs:
            if params:
                curs.execute(sql, params)
            else:
                curs.execute(sql)
            output = []
            columns = [column[0] for column in curs.description]
            for row in curs.fetchall():
                output.append(dict(zip(columns, row)))
        else:
            output = self._get_connection(sql, params, self.get_sql_list_dicts)
        return output

    def get_sql_single_item_list(self, sql: str, params: list = None, curs=False, conn=False) -> list:
        """
        Returns a single column list for a given SQL Query

        Args:
            sql: SQL string
            params: List of parameters
            curs: An instance of a database cursor. Will be false when method is first called, then populated when
                  method is called recursively.
            conn: An instance of a database connection or false on first call.

        Returns: A list containing the results of the query
        """
        self._print_debug_output(f"Getting query:\n {sql}")

        if curs:
            if params:
                curs.execute(sql, params)
            else:
                curs.execute(sql)
            output = []
            for row in curs.fetchall():
                output.append(row[0])
        else:
            output = self._get_connection(sql, params, self.get_sql_single_item_list)
        return output

    def execute_simple(self, sql: str, params: list = None, curs=False, conn=False):
        """
        Execute as single SQL statement

        Args:
            sql: SQL string
            params: List of parameters
            curs: An instance of a database cursor. Will be false when method is first called, then populated when
                  method is called recursively.
            conn: An instance of a database connection or false on first call.

        Returns: None
        """
        if curs:
            self._print_debug_output(f"Getting query:\n {sql}")
            curs.execute(sql, params)
            self._print_debug_output('csr.execute complete.')
            conn.commit()
            self._print_debug_output('conn.commit complete.')
        else:
            self._get_connection(sql, params, self.execute_simple)

    def get_single_result(self, sql: str, params: list = None, curs=False, conn=False):
        """
        Execute as single SQL statement

        Args:
            sql: SQL string
            params: List of parameters
            curs: An instance of a database cursor. Will be false when method is first called, then populated when
                  method is called recursively.
            conn: An instance of a database connection or false on first call.

        Returns: None
        """
        if curs:
            self._print_debug_output(f"Getting query:\n {sql}")
            curs.execute(sql, params)
            output = curs.fetchone()
        else:
            output = self._get_connection(sql, params, self.get_single_result)
        if isinstance(output, tuple):
            return output[0]
        else:
            return output

    def execute_batch(self, sql: str, params: list, curs=False, conn=False, page_size: int = 1000) -> None:
        """
        Executes batches of SQL Queries
        Args:
            sql: SQL string
            params: List of parameters
            curs: An instance of a database cursor. Will be false when method is first called, then populated when
                  method is called recursively.
            conn: An instance of a database connection or false on first call.
            page_size: Page size controls the number of records pushed in each batch.

        Returns: None
        """
        self._page_size = self._page_size if self._page_size else page_size
        start_time = time.time()
        self._print_debug_output(f"Execute Batches: Inserting {len(params)} records")
        self._print_debug_output(f"Getting query:\n {sql}")
        params = self.convert_nan_to_none(params)
        if curs:
            sql_ = []
            for i in params:
                sql_.append(sql.format(*i))
            sql_ = '; '.join(sql_)
            curs.execute(sql_)
            duration = time.time() - start_time
            self._print_debug_output(f'Updated {len(params)} rows in {round(duration, 2)} seconds')
            conn.commit()
        else:
            self._get_connection(sql, params, self.execute_batch)

    @staticmethod
    def _fix_missing_parenthesis(sql: str) -> str:
        """
        Adds missing parenthesis around the %s in an update statement.

        For most psycopg2 sql statements the format would be 'update db.table (name) values %s'. For some reason in the
        case of update batches the format needs a () around the %s. Example 'update db.table (name) values (%s)'.

        Args:
            sql: A string containing the SQL statement.

        Returns:
            A string with parenthesis added if needed.

        """
        x = sql.split('%s')
        if x[0][-1] != '(':
            sql_ = sql.replace('%s', '(%s)')
        else:
            sql_ = sql
        return sql_

    def insert_many(self, sql: str, params: list, curs=False, conn=False) -> None:
        """
        Executes a SQL Query

        Args:
            sql: SQL string
            params: List of parameters
            curs: An instance of a database cursor. Will be false when method is first called, then populated when
                  method is called recursively.
            conn: An instance of a database connection or false on first call.

        Returns: None
        """

        params = self.convert_nan_to_none(params)
        if curs:
            start_time = time.time()
            self._print_debug_output(f"Execute Many: Inserting {len(params)} records")
            self._print_debug_output(f"Getting query:\n {sql}")
            execute_values(curs, sql, params)
            duration = time.time() - start_time
            self._print_debug_output(f'Inserted {len(params)} rows in {round(duration, 2)} seconds')
            conn.commit()
        else:
            self._get_connection(sql, params, self.insert_many)

    # look up alias decorator
    def execute_many(self, sql: str, params: list, curs=False, conn=False) -> None:
        """
        Executes a SQL Query

        Args:
            sql: SQL string
            params: List of parameters
            curs: An instance of a database cursor. Will be false when method is first called, then populated when
                  method is called recursively.
            conn: An instance of a database connection or false on first call.

        Returns: None
        """
        print("Warning: execute_many will be deprecated. Please use insert_many instead.")
        self.insert_many(sql, params, curs, conn)

    def update_batch_from_dataframe(self, schema_table: str, df: pd.DataFrame, page_size: int = 1000) -> None:
        """

        Args:
            schema_table: schema.table to be used
            df: Dataframe where there should be one index column used as a unique identifier. the rest of the columns
            contains values to be updated within the databse.
            page_size: pagenation size to be used.

        Returns:

        """
        params = df.to_dict(orient='records')
        self.update_batch(schema_table, params, page_size=page_size)

    def update_batch(self, schema_table: str, params: List[Dict[int, any]], curs=False, conn=False,
                     page_size: int = 1000) -> None:
        """

        Args:
            schema_table:
            params: [{'id': 4, 'anything': 'some value', 'another_col': 42}]
            curs:
            conn:
            page_size: Page size controls the number of records pushed in each batch.

        Returns: None
        """
        self._page_size = self._page_size if self._page_size else page_size
        if curs:
            start_time = time.time()
            row_set_str = ''
            row_execute_str = ''
            counter = 1
            first_row = params[0]
            for k, val in first_row.items():
                # 'set col_name=$1'
                # "EXECUTE updateStmt (%(msg)s, %(id)s)"
                if k != 'id':
                    row_set_str = f"{row_set_str} {k}=${counter},"
                    row_execute_str = f'{row_execute_str}%({k})s,'
                    counter += 1
            row_set_str = row_set_str.rstrip(',')
            row_execute_str = row_execute_str.rstrip(',')
            execute_str = f'execute updateStmt ({row_execute_str}, %(id)s)'
            prepared_statement = f'prepare updateStmt as update {schema_table} set{row_set_str} where id=${counter}'

            curs.execute(prepared_statement)
            execute_batch(curs, execute_str, params, page_size=page_size)
            curs.execute("DEALLOCATE updateStmt")
            conn.commit()
            duration = time.time() - start_time
            self._print_debug_output(f'updated {len(params)} rows in {round(duration, 2)} seconds')
        else:
            self._get_connection(schema_table, params, self.update_batch)

    def delete(self, sql: str, params: list):
        """
        Deletes data

        Args:
            sql: SQL string
            params: List of parameters

        Returns: None
        """
        print('WARNING: Depreciated function. Use execute_simple.')
        self.execute_simple(sql, params)

    # Utility methods
    @staticmethod
    def make_column_names(columns: List[str]) -> str:
        """
        Creates the column name comma sep values
        Args:
            columns: A list of strings containing the column names

        Returns: A comma separated string representing the columns for inclusion in a SQL statement.
        """
        return ','.join(columns)

    @staticmethod
    def make_param_string(input_list: List[str]) -> str:
        """
        Creates the %s for a query string
        Args:
            input_list: A list containing the parameters for a SQL call


        Returns: A string with parameters represented by %s placeholders.
        """
        list_len = len(input_list)
        param_string = ""
        x = 0
        while x < list_len:
            param_string += '%s, '
            x += 1
        param_string = param_string[:-2]
        return param_string

    def make_variable_replacements(self, input_list: List[str]) -> str:
        """
        Creates the question marks for a query string.

        This method has been replaced by make_param_string.

        Args:
            input_list: A list containing the parameters for a SQL call

        Returns: A string with parameters represented by %s placeholders.
        """
        print('make_variable_replacements has been depreciated and will be removed in a future version.')
        return self.make_param_string(input_list)

    def build_sql_from_dataframe(self, df: pd.DataFrame, table_name: str, schema: str) -> Tuple[str, list]:
        """
        Builds a SQL string and params from dataframe
        This method will return a string SQL call and a list of params. The column names of the
        dataframe must match the column names in the table.
        Args:
            df: DataFrame to use for SQL call
            table_name: Name of the target table
            schema: Name of the target schema

        Returns:
            A sql string template for the insert statement.
            A list of params
        """
        columns = list(df.columns)
        columns_str = self.make_column_names(columns)
        schema = f'{schema}.' if schema else ''
        sql = f'insert into {schema}{table_name} ({columns_str}) values %s;'
        vals = list(df.values)
        # This returns a list of arrays. Need to convert to list of lists.
        params = [list(x) for x in vals]
        return sql, params

    @staticmethod
    def convert_nan_to_none(params: List[Any]) -> List[Any]:
        """
        Converts all instances of nan to None.
        Some servers cannot handle pd.nan. It will throw an error. This function converts all
        nan to None. Panda's built in null replacement functions are just not reliable.

        Args:
            params: A list of parameters

        Returns: A list of parameters with pd.nan's converted to None
        """
        row_counter = 0
        for row in params:
            value_counter = 0
            if isinstance(row, list):
                for v in row:
                    # A note on inf. inf, or np.inf shows up sometimes. It can be positive or negative (oddly).
                    # It's important to remove this or SQL Server will throw an error about floating point precision.
                    if pd.isnull(v) or v == inf or v == -inf:
                        row[value_counter] = None
                    value_counter += 1
                params[row_counter] = row
            else:
                # The params are not a multi-dimensional list.
                if pd.isnull(row):
                    row = None
                params[row_counter] = row

            row_counter += 1
        return params

    # def update_db(self, df: pd.DataFrame, update_cols: list, static_cols: list, schema: str, table: str) -> None:
    #     """
    #     Returns a list of static values and a list of updated values that will be used as inputs
    #     within update_write_to_db
    #
    #     Args:
    #         df: A dataframe where each record contains a new value that will replace a value in a relational db.
    #         update_cols: Column names which contain the values a user will replace in a relational db.
    #         static_cols: Column names used as a unique identifier to update data in relational db.
    #         schema: The schema for the table to replace values in
    #         table: Table name to replace values in
    #     Returns:
    #         None
    #     """
    #     df_ = df[update_cols + static_cols].drop_duplicates()
    #     df_ = df_.where(pd.notnull(df_), None)
    #     updated_statements = []
    #     static_statements = []
    #
    #     for i in range(len(df_)):
    #         updated_col_val = []
    #         static_col_val = []
    #
    #         update_val = df_.loc[i, update_cols].values
    #         static_val = df_.loc[i, static_cols].values
    #
    #         for up_col, up_val in zip(update_cols, update_val):
    #             updated_col_val.append(self._set_column_value(up_col, up_val, ','))
    #         updated_col_val = ''.join(updated_col_val)
    #         updated_statements.append(updated_col_val)
    #
    #         for st_col, st_val in zip(static_cols, static_val):
    #             static_col_val.append(self._set_column_value(st_col, st_val, ' and'))
    #         static_col_val = ''.join(static_col_val)
    #         static_statements.append(static_col_val)
    #
    #     updated_statements = [x.rstrip(', ') for x in updated_statements]
    #     static_statements = [x.rstrip('and ') for x in static_statements]
    #
    #     sql = []
    #     for ss, us in zip(static_statements, updated_statements):
    #         sql.append(f"""update {schema}.{table} set  {us} where {ss};""")
    #     sql = ' '.join(sql)
    #     self.execute_simple(sql)
    #
    # @staticmethod
    # def _set_column_value(col, val, sep):
    #     if isinstance(val, str):
    #         return f"{col}='{val}'{sep} "
    #
    #     elif isinstance(val, datetime):
    #         if pd.isnull(val):
    #             return ''
    #         else:
    #             return f"{col}='{val}'{sep} "
    #
    #     elif pd.isnull(val) or val is None:
    #         return ''
