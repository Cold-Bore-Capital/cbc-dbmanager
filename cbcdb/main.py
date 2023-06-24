import random
import socket
import time
from datetime import datetime, date
from typing import List, Any, Dict, Tuple

from configservice.config import Config
from psycopg2 import connect
from psycopg2.extras import execute_values


class DBManager:
    """
    DBManager handled the read and write information to the DB.
    This class handles the transport of data to and from the db. The goal is to use
    highly reusable methods, shareable across most of the classes.
    """

    def __init__(self,
                 use_aws_secrets=True,
                 profile_name=None,
                 secret_name=None,
                 region_name=None,
                 aws_cache=None,
                 debug_output_mode=None,
                 db_name=None,
                 db_user=None,
                 db_password=None,
                 db_schema=None,
                 db_host=None,
                 db_port=None,
                 test_mode=False):
        """
        Init Function

        Args:
            debug_output_mode: Flag to turn on debug mode. Setting this to True will print debug messages.
            db_name:
            db_user:
            db_password:
            db_schema:
            db_host:
            db_port:
            test_mode:
        """
        self._config = Config(profile_name=profile_name,
                              secret_name=secret_name,
                              aws_secrets=use_aws_secrets,
                              region_name=region_name,
                              test_mode=test_mode)
        if use_aws_secrets:
            if aws_cache:
                self._config.get_all_secrets()

            self._debug_mode = debug_output_mode
            self._db_host = db_host if db_host else self._config.get_secret('DB_HOST')
            self._db_name = db_name if db_name else self._config.get_secret('DB_NAME')
            self._db_user = db_user if db_user else self._config.get_secret('DB_USER')
            self._db_password = db_password if db_password else self._config.get_secret('DB_PASSWORD')
            self._db_schema = db_schema if db_schema else self._config.get_secret('DB_SCHEMA')
            # Publicly accessible schema
            self.db_schema = self._db_schema
            if db_port:
                self._db_port = db_port if db_port else self._config.get_secret('DB_PORT', data_type_convert='int')
            else:
                self._db_port = self._config.get_secret('DB_PORT', data_type_convert='int')

            self._page_size = None
        else:
            # self._debug_mode = self.debug_output_mode if not debug_output_mode else debug_output_mode
            self._debug_mode = debug_output_mode
            self._db_host = db_host if db_host else self._config.get_env('DB_HOST')
            self._db_name = db_name if db_name else self._config.get_env('DB_NAME')
            self._db_user = db_user if db_user else self._config.get_env('DB_USER')
            self._db_password = db_password if db_password else self._config.get_env('DB_PASSWORD')
            # @todo Implement this as a default schema.
            self._db_schema = db_schema if db_schema else self._config.get_env('DB_SCHEMA')
            # Publicly accessible schema
            self.db_schema = self._db_schema
            if db_port:
                self._db_port = db_port if db_port else self._config.get_env('DB_PORT', data_type_convert='bool')
            else:
                self._db_port = self._config.get_env('DB_PORT', data_type_convert='bool')

            self._page_size = None

        # Convert DB port if needed.
        if not isinstance(self._db_port, int):
            self._db_port = int(self._db_port)

    def _get_random_port(self, port):
        if port == 'random':
            while 1 == 1:
                rand_port = random.randint(5000, 50000)
                if not self.is_port_in_use(rand_port):
                    return rand_port
        elif port:
            return int(port)

    @staticmethod
    def is_port_in_use(port: int):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', port)) == 0

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

    def get_sql_dataframe(self, sql: str, params: list = None, curs=False, conn=False):
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
        import pandas as pd
        if conn:
            self._print_debug_output(f"Getting query:\n {sql}")
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
        if curs:
            self._print_debug_output(f"Getting query:\n {sql}")
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
        if curs:
            self._print_debug_output(f"Getting query:\n {sql}")
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
        params = self.convert_nan_to_none(params)
        if curs:
            self._print_debug_output(f"Execute Batches: Inserting {len(params)} records")
            self._print_debug_output(f"Getting query:\n {sql}")
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

    def update_batch_from_df(self, df, update_cols: list, static_cols: list, schema: str,
                             table: str) -> None:
        """
        Generates and executes an update from a dataframe.


        Args:
            df: A dataframe where each record contains a new value that will replace a value in a relational db.
            update_cols: Column names which contain the values a user will replace in a relational db.
            static_cols: Column names used as a unique identifier to update data in relational db.
            schema: The schema for the table to replace values in
            table: Table name to replace values in
        Returns:
            None
        """
        import pandas as pd
        df_ = df[update_cols + static_cols].drop_duplicates()
        df_ = df_.where(pd.notnull(df_), None)
        updated_statements = []
        static_statements = []

        quote_col_dict = self._get_table_column_dtypes(schema, table, list(df_.columns))

        for i in range(len(df_)):
            updated_col_val = []
            static_col_val = []

            update_val = df_.loc[i, update_cols].values
            static_val = df_.loc[i, static_cols].values

            for up_col, up_val in zip(update_cols, update_val):
                updated_col_val.append(self._set_column_value(up_col, up_val, ',', quote_col_dict))
            updated_col_val = ''.join(updated_col_val)
            updated_statements.append(updated_col_val)

            for st_col, st_val in zip(static_cols, static_val):
                static_col_val.append(self._set_column_value(st_col, st_val, ' and', quote_col_dict))
            static_col_val = ''.join(static_col_val)
            static_statements.append(static_col_val)

        updated_statements = [x.rstrip(', ') for x in updated_statements]
        static_statements = [x.rstrip('and ') for x in static_statements]

        sql = []
        for ss, us in zip(static_statements, updated_statements):
            sql.append(f"""update {schema}.{table} set  {us} where {ss};""")
        sql = ' '.join(sql)
        self.execute_simple(sql)

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

    # Section Utility methods
    def _get_table_column_dtypes(self, schema: str, table: str, columns: List[str]) -> dict:
        """
        Queries the redshift/postgres database and requests information on the table data types. Returns a dict
        containing each column name, and a quoted column with values 1 or 0.

        Executes a query like:
            select column_name, data_type from information_schema.columns
            where table_schema = 'bi' and table_name = 'color';
        Args:
            schema: The name of the schema to query.
            table: The name of the table to query.
            columns: A list of columns found in the incoming update query.

        Returns:
            A dict where 1 = quoted, 0 = unquoted. Structure: {'id': 0, 'name': 1, 'age': 0, 'start_date': 1}
        """
        sql = f"""select column_name, data_type from information_schema.columns
                  where table_schema = '{schema}' and table_name = '{table}';"""
        res = self.get_sql_list_dicts(sql)
        dtypes = {x['column_name']: x['data_type'] for x in res}
        quoted_types = ['character varying', 'nvarchar', 'text', 'character', 'nchar', 'bpchar', 'date',
                        'timestamp without time zone', 'timestamp with time zone', 'time without time zone',
                        'time with time zone']
        unquoted_types = ['numeric', 'bigint', 'smallint', 'integer', 'bool', 'float4', 'float8', 'float', 'real',
                          'double precision', 'boolean']
        output = {}
        for col in columns:
            quote_val = None
            dtype = dtypes.get(col)
            if not dtype:
                raise MissingDatabaseColumn(f'The column {col} was not found in the {table} table.')
            if dtype in quoted_types:
                quote_val = 1
            elif dtype in unquoted_types:
                quote_val = 0
            else:
                raise MissingDTypeFromTypes(f'The column {col} in {table} table has a data type of {dtype}.'
                                            f' This data type is not found in the quoted_types or unquoted_types list.')
            output[col] = quote_val
        return output

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

    def build_sql_from_dataframe(self, df, table_name: str, schema: str) -> Tuple[str, list]:
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
        import pandas as pd
        columns = list(df.columns)
        columns_str = self.make_column_names(columns)
        schema = f'{schema}.' if schema else ''
        sql = f'insert into {schema}{table_name} ({columns_str}) values %s;'
        vals = list(df.values)
        # This returns a list of arrays. Need to convert to list of lists.
        params = [list(x) for x in vals]
        return sql, params

    def save_dataframe(self, df, table_name: str, schema: str) -> None:
        """
        Saves a dataframe to a table in redshift/postgres.
        Args:
            df: DataFrame to use for SQL call
            table_name: Name of the target table
            schema: Name of the target schema.
        Returns:
            None
        """
        sql, params = self.build_sql_from_dataframe(df, table_name, schema)
        self.insert_many(sql, params)

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
        import pandas as pd
        from numpy import inf
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

    @staticmethod
    def _set_column_value(col: str, val: str, sep: str, quote_flag_dict: dict) -> str:
        """
        Returns a string for the "set" section of the update statement in the appropriate format based on datatype.

        Args:
            col: The name of the column. i.e. 'first_name'
            val: The value, i.e. 'Craig'
            sep: The type of separator to use, typically either a ',' when setting a value or 'and' for a filter.
            quote_flag_dict: A dict containing column names as the key, and a 1 or 0 flag for quote. For example:
                           {'first_name': 1, 'age': 0}.

        Returns:
            A string with the set or filter text. Example return formats:
                1. Set portion string type value "col = 'val',"
                2. Set portion numeric or bool value "col = val,"
                3. Set portion string type value "col = 'val' and"
                4. Set portion numeric or bool value "col = val and"
        """
        import pandas as pd
        if pd.isnull(val):
            return f"{col}=null{sep} "
        if quote_flag_dict[col]:
            # Value should be wrapped in quotes
            # Check if value is a datetime or date type.

            if isinstance(val, pd.Timestamp) or isinstance(val, datetime):
                # if val.tzinfo is not None and val.tzinfo.utcoffset(val) is not None:
                #     # Timezone is present
                #
                # else:

                # It looks like the timezone will default to an empty string if not set. No need for anything fancy.
                val = val.strftime('%Y-%m-%dT%H:%M:%S%z')
            elif isinstance(val, date):
                val = val.strftime('%Y-%m-%d')
            return f"{col}='{val}'{sep} "
        else:
            # Value is numeric or bool
            if isinstance(val, bool):
                # If Bool, convert to a 1 or 0 for consistency.
                val = 1 if val else 0
            return f"{col}={val}{sep} "


class MissingDatabaseColumn(Exception):
    pass


class MissingDTypeFromTypes(Exception):
    pass
