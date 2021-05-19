from unittest import TestCase
import pandas as pd
# import psycopg2
from cbcdb.main import DBManager


class TestDBManager(TestCase):

    @staticmethod
    def _get_db_inst():
        db = DBManager(
            debug_output_mode=True,
            use_ssh=False,
            ssh_remote_bind_port=5434,
            db_name='test',
            db_user='test',
            db_password='test',
            db_schema='public',
            db_host='localhost')
        return db

    def _make_some_data(self, db):
        # Check if the table exists
        test_table_name = 'color'

        sql = f"""
        select exists(
               SELECT
               FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public'
                 AND c.relname = '{test_table_name}'
           );"""

        res = db.get_single_result(sql)

        if res:
            # Delete the database
            sql = f"drop table public.{test_table_name}"
            db.execute_simple(sql)

        # sql = """
        # create table public.testing
        #        (id             serial,
        #         test           varchar(10),
        #         a_number       smallint,
        #         firstname      varchar(75),
        #         lastname       varchar(75),
        #         primary key (id));"""

        sql = f"""CREATE TABLE public.{test_table_name} (
                    color_id serial,
                    color_name VARCHAR NOT NULL
                );"""

        db.execute_simple(sql)
        sql = f"""
        insert into public.{test_table_name}(color_name)
        values ('red'),('green'),('pink'),('purple'),('blue'),('cyan')
        """
        db.execute_simple(sql)

        return test_table_name

    # def test__get_connection(self):
    #     db = self._get_db_inst()
    #     conn = db._get_connection()
    #     self.assertEqual(0, conn.closed)
    #     # db._safe_tunnel_close(conn)

    def test__get_sql_dataframe(self):
        db = self._get_db_inst()
        table_name = self._make_some_data(db)

        res = db.get_sql_dataframe(f'select * from {table_name}')
        golden = 'red'
        test = res.iloc[0]['color_name']
        self.assertEqual(golden, test)

        # @todo test with params.

    def test__get_sql_list_dicts(self):
        db = self._get_db_inst()
        table_name = self._make_some_data(db)

        res = db.get_sql_list_dicts(f'select * from {table_name}')
        golden = 'red'
        test = res[0]['color_name']
        self.assertEqual(golden, test)

    def test__get_sql_single_item_list(self):
        db = self._get_db_inst()
        table_name = self._make_some_data(db)
        res = db.get_sql_single_item_list(f'select color_name from {table_name}')
        golden = 'red'
        test = res[0]
        self.assertEqual(golden, test)

    def test__execute_simple(self):
        db = self._get_db_inst()
        table_name = self._make_some_data(db)
        sql = f"""
        insert into public.{table_name}(color_name)
        values ('abc')
        """
        db.execute_simple(sql)
        res = db.get_sql_single_item_list(f'select color_name from {table_name}')
        golden = 'abc'
        test = res[len(res)-1]
        self.assertEqual(golden, test)

    def test__get_single_result(self):
        db = self._get_db_inst()
        table_name = self._make_some_data(db)
        test = db.get_single_result(f'select color_name from {table_name}')
        golden = 'red'
        self.assertEqual(golden, test)

    def test__execute_many(self):
        db = self._get_db_inst()
        table_name = self._make_some_data(db)

        # Test normal function
        sql = 'insert into public.color (color_name) values %s;'
        params = [['ivory'], ['lemon'], ['copper'], ['salmon'], ['rust'], ['amber'], ['cream'], ['tan'], ['bronze'], ['blue'], ['silver'], ['grey']]
        db.execute_many(sql, params)

        res = db.get_sql_single_item_list(f'select color_name from {table_name}')

        golden = 'grey'
        test = res[len(res)-1]
        self.assertEqual(golden, test)

        # Test errors - Bad column name
        sql = 'insert into public.color (column_that_doesnt_exist) values %s;'
        failed = False
        try:
            db.execute_many(sql, params)
        except Exception as e:
            failed = True
        self.assertTrue(failed)

        # Syntax error (intos vs into)
        sql = 'insert intos public.color (color_name) values %s;'
        failed = False
        try:
            db.execute_many(sql, params)
        except Exception as e:
            failed = True
        self.assertTrue(failed)


    def test__build_sql_from_dataframe(self):
        db = self._get_db_inst()
        df = pd.DataFrame({'color_name':['ivory', 'lemon', 'copper', 'salmon', 'rust', 'amber', 'cream', 'tan',
                                         'bronze', 'blue', 'silver', 'grey']})

        sql, params = db.build_sql_from_dataframe(df, 'color', 'public')
        golden = 'insert into public.color (color_name) values %s;'
        self.assertEqual(golden, sql)

        golden = ['ivory']
        self.assertEqual(golden, params[0])

