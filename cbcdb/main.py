import time
from typing import List, Any, Dict, Tuple
from psycopg2 import connect
from psycopg2.extras import execute_values
import sshtunnel
import pandas as pd
from numpy import inf


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
            ssh_local_bind_port:
            db_name:
            db_user:
            db_password:
            db_schema:
            db_host:
        """
        self._config = Config()
        self._debug_mode = self._config.debug_output_mode if not debug_output_mode else debug_output_mode
        self._db_host = db_host if db_host else self._config.db_host
        self._db_name = db_name if db_name else self._config.db_name
        self._db_user = db_user if db_user else self._config.db_user
        self._db_password = db_password if db_password else self._config.db_password
        # @todo Implement this as a default schema.
        self._db_schema = db_schema if db_schema else self._config.db_schema
        self._db_port = ssh_remote_bind_port if ssh_remote_bind_port else self._config.ssh_remote_bind_port

        self.use_ssh = use_ssh if use_ssh else self._config.use_ssh
        if self.use_ssh:
            ssh_host = ssh_host if ssh_host else self._config.ssh_host
            ssh_port = ssh_port if ssh_port else self._config.ssh_port
            ssh_user = ssh_user if ssh_user else self._config.ssh_user
            ssh_key_path = ssh_key_path if ssh_key_path else self._config.ssh_key_path
            ssh_remote_bind_port = ssh_remote_bind_port if ssh_local_bind_port else self._config.ssh_remote_bind_port
            ssh_local_bind_address = ssh_local_bind_address if ssh_local_bind_address else self._config.ssh_local_bind_address
            ssh_local_bind_port = ssh_local_bind_port if ssh_local_bind_port else self._config.ssh_local_bind_port

        if self.use_ssh:
            self.tunnel = sshtunnel.SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_user,
                ssh_pkey=ssh_key_path,
                remote_bind_address=(self._db_host, ssh_remote_bind_port),
                local_bind_address=(ssh_local_bind_address, ssh_local_bind_port))

    def _print_debug_output(self, msg: str):
        """
        Prints a debug message if system is in debug mode.
        Args:
            msg: A string containing the message to print to the logs

        Returns: None

        """
        if self._debug_mode:
            print(f'DEBUG: {msg}')

    def _get_connection(self):
        con = None
        if self.use_ssh:
            if not self.tunnel.is_active:
                self._print_debug_output("Starting tunnel connection")
                self.tunnel.start()
                self._print_debug_output(f"Tunnel status {self.tunnel.check_tunnels()}")
            host = self.tunnel.local_bind_host
            port = self.tunnel.local_bind_port
        else:
            host = self._db_host
            port = self._db_port

        try:
            con = connect(dbname=self._db_name,
                          host=host,
                          port=port,
                          user=self._db_user,
                          password=self._db_password)
            self._print_debug_output(f'Got database connection with status {con.status}')
            return con
        except ConnectionError as e:
            print("ERROR: Encountered an error establishing connection")
            if con:
                con.rollback()
                con.close()
            self.tunnel.stop()
            raise ConnectionError(e)

    def _safe_tunnel_close(self):
        """
        Safely shuts down the connection and tunnel in the event of a problem.

        Args:
            conn: A connection instance to the database.
            csr: A cursor instance.
            erro_mode: A flag indicating if this shutdown is due to an error. A rollback() will be called if True.

        Returns: None

        """
        # if error_mode:
        #     print("WARNING: Safe shutdown of DB connection and SSH tunnel initiated.")
        # try:
        #     if csr:
        #         csr.close()
        #         del csr
        #     if error_mode:
        #         conn.rollback()
        #     conn.close()
        #     self._print_debug_output("Connection closed.")
        # except Exception as e:
        #     print('Failed to close database connection: \n {e}')

        if self.use_ssh:
            try:
                if self.tunnel.is_active:
                    self._print_debug_output('Starting Tunnel Close.')
                    self.tunnel.stop()
                    self._print_debug_output('SSH Tunnel closed.')
                else:
                    self._print_debug_output('Attempted to close tunnel but it was already inactive.')
            except Exception as e:
                print('Failed to close SSH tunnel: \n {e}')

    def get_sql_dataframe(self, sql: str, params: list = None) -> pd.DataFrame:
        """
         Returns a DataFrame for a given SQL query

        Args:
            sql: SQL string
            params: List of parameters

        Returns: A Pandas Dataframe.

        """
        self._print_debug_output(f"Getting query:\n {sql}")

        conn = self._get_connection()
        with conn:
            try:
                if params:
                    df = pd.read_sql_query(sql, params=params, con=conn)
                else:
                    df = pd.read_sql_query(sql, con=conn)
            except Exception as e:
                self._safe_tunnel_close()
                raise Exception(f'Database Read Attempt Failed \n {e}')
            finally:
                self._safe_tunnel_close()
        return df

    def get_sql_list_dicts(self, sql: str, params: list = None) -> List[Dict[str, Any]]:
        """
        Returns a list of dicts for a given SQL Query

        Example Output:
            [{'some': 'data'},
             {'more': 'otherdata'}]

        Args:
            sql: SQL string
            params: Parameters for SQL call

        Returns:

        """
        self._print_debug_output(f"Getting query:\n {sql}")
        conn = self._get_connection()
        with conn:
            with conn.cursor() as curs:
                try:
                    if params:
                        curs.execute(sql, params)
                    else:
                        curs.execute(sql)
                    output = []
                    columns = [column[0] for column in curs.description]
                    for row in curs.fetchall():
                        output.append(dict(zip(columns, row)))
                except Exception as e:
                    self._safe_tunnel_close()
                    raise Exception(f'Database Read Attempt Failed \n {e}')
                finally:
                    self._safe_tunnel_close()
        return output

    def get_sql_single_item_list(self, sql: str, params: list = None) -> list:
        """
        Returns a single column list for a given SQL Query

        Args:
            sql: SQL string
            params: List of parameters

        Returns: A list containing the results of the query
        """
        self._print_debug_output(f"Getting query:\n {sql}")

        conn = self._get_connection()
        with conn:
            with conn.cursor() as curs:
                try:
                    if params:
                        curs.execute(sql, params)
                    else:
                        curs.execute(sql)
                    output = []
                    for row in curs.fetchall():
                        output.append(row[0])
                except Exception as e:
                    self._safe_tunnel_close()
                    raise Exception(f'Database Read Attempt Failed \n {e}')
                finally:
                    self._safe_tunnel_close()
        return output

    def execute_simple(self, sql: str, params: list = None):
        """
        Execute as single SQL statement

        Args:
            sql: SQL string
            params: List of parameters

        Returns: None
        """
        conn = self._get_connection()
        with conn:
            with conn.cursor() as curs:
                self._print_debug_output(f"Getting query:\n {sql}")
                try:
                    curs.execute(sql, params)
                    self._print_debug_output('csr.execute complete.')
                    conn.commit()
                    self._print_debug_output('conn.commit complete.')
                except Exception as e:
                    self._safe_tunnel_close()
                    raise Exception(f'Database Execute Attempt Failed \n {e}')
                finally:
                    self._safe_tunnel_close()

    def get_single_result(self, sql: str, params: list = None):
        """
        Execute as single SQL statement

        Args:
            sql: SQL string
            params: List of parameters

        Returns: None
        """
        conn = self._get_connection()
        with conn:
            with conn.cursor() as curs:
                self._print_debug_output(f"Getting query:\n {sql}")
                try:
                    curs.execute(sql, params)
                    res = curs.fetchone()
                except Exception as e:
                    self._safe_tunnel_close()
                    raise Exception(f'Database Execute Attempt Failed \n {e}')
                finally:
                    self._safe_tunnel_close()
        return res

    def execute_many(self, sql: str, params: list) -> None:
        """
        Executes a SQL Query

        Args:
            sql: SQL string
            params: List of parameters

        Returns: None
        """
        start_time = time.time()
        self._print_debug_output(f"Execute Many: Inserting {len(params)} records")
        self._print_debug_output(f"Getting query:\n {sql}")
        params = self.convert_nan_to_none(params)
        conn = self._get_connection()
        with conn:
            with conn.cursor() as curs:
                try:
                    execute_values(curs, sql, params)
                    duration = time.time() - start_time
                    self._print_debug_output(f'Inserted {len(params)} rows in {round(duration, 2)} seconds')
                    conn.commit()
                except Exception as e:
                    self._safe_tunnel_close()
                    raise Exception(f'Database Execute Attempt Failed \n {e}')
                finally:
                    self._safe_tunnel_close()

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
