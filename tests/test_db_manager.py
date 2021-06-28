# import psycopg2
import random
from unittest import TestCase

import pandas as pd

from cbcdb.main import DBManager
from tests.docker_test_setup import start_pg_container


class TestDBManager(TestCase):

    def setUp(self):
        start_pg_container()

    def tearDown(self):
        # This would shut down the container, but it's such a pain in the ass during testing.
        # shutdown_pg_container()
        pass

    @staticmethod
    def _get_db_inst(test_env_defaults=False):
        if test_env_defaults:
            db = DBManager()
        else:
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

    @staticmethod
    def _prepare_test_table(db, no_data_flag=False):
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
                    id serial,
                    color_name VARCHAR NOT NULL,
                    another_value VARCHAR
                );"""

        db.execute_simple(sql)

        if not no_data_flag:
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
        table_name = self._prepare_test_table(db)

        res = db.get_sql_dataframe(f'select * from {table_name}')
        golden = 'red'
        test = res.iloc[0]['color_name']
        self.assertEqual(golden, test)

        # @todo test with params.

    def test__get_sql_list_dicts(self):
        db = self._get_db_inst()
        table_name = self._prepare_test_table(db)

        res = db.get_sql_list_dicts(f'select * from {table_name}')
        golden = 'red'
        test = res[0]['color_name']
        self.assertEqual(golden, test)

    def test__get_sql_single_item_list(self):
        db = self._get_db_inst()
        table_name = self._prepare_test_table(db)
        res = db.get_sql_single_item_list(f'select color_name from {table_name}')
        golden = 'red'
        test = res[0]
        self.assertEqual(golden, test)

    def test__execute_simple(self):
        db = self._get_db_inst()
        table_name = self._prepare_test_table(db)
        sql = f"""
        insert into public.{table_name}(color_name)
        values ('abc')
        """
        db.execute_simple(sql)
        res = db.get_sql_single_item_list(f'select color_name from {table_name}')
        golden = 'abc'
        test = res[len(res) - 1]
        self.assertEqual(golden, test)

    def test__get_single_result(self):
        db = self._get_db_inst()
        table_name = self._prepare_test_table(db)
        test = db.get_single_result(f'select color_name from {table_name}')
        golden = 'red'
        self.assertEqual(golden, test)

    def test__execute_batch(self):
        db = self._get_db_inst()
        table_name = self._prepare_test_table(db, True)
        num_rows = 10000
        col1 = self.create_single_row_procedural_data(num_rows)
        col2 = self.create_single_row_procedural_data(num_rows)
        params = list(zip(col1, col2))
        sql = 'insert into public.color (color_name, another_value) values %s'
        db.insert_many(sql, params)

        col1 = self.create_single_row_procedural_data(num_rows)
        col2 = self.create_single_row_procedural_data(num_rows)
        ids = list(range(num_rows))
        params = list(zip(col1, col2, ids))
        # old
        sql = 'update public.color set color_name={0}, another_value={1} where id={2}'
        # new
        sql = "update public.color set color_name='{0}', another_value='{1}' where id={2}"
        db.execute_batch(sql, params)

        # # Syntax error (intos vs into)
        # sql = 'insert intos public.color (color_name) values %s;'
        # failed = False
        # try:
        #     db.insert_many(sql, params)
        # except Exception as e:
        #     failed = True
        # self.assertTrue(failed)

    def test_update_many(self):
        # Update time notes: 50K in
        db = self._get_db_inst()
        table_name = self._prepare_test_table(db, True)
        num_rows = 10000
        col1 = self.create_single_row_procedural_data(num_rows)
        col2 = self.create_single_row_procedural_data(num_rows)
        params = list(zip(col1, col2))
        sql = 'insert into public.color (color_name, another_value) values %s'
        db.insert_many(sql, params)

        params = []
        for x in range(num_rows):
            params.append({'id': x, 'color_name': f'red_{x}', 'another_value': f'blue_{x}'})

        db.update_batch('public.color', params, page_size=num_rows)

    def test__update_batch_from_dataframe(self):
        # Update time notes: 50K in
        db = self._get_db_inst()
        table_name = 'public.' + self._prepare_test_table(db, True)
        num_rows = 10000
        col1 = self.create_single_row_procedural_data(num_rows)
        col2 = self.create_single_row_procedural_data(num_rows)
        params = list(zip(col1, col2))
        sql = 'insert into public.color (color_name, another_value) values %s'
        db.insert_many(sql, params)

        params = []
        randomlist = random.sample(range(0, 9999), 1000)
        for x in randomlist:
            params.append({'id': x, 'color_name': f'red_{x}', 'another_value': f'blue_{x}'})

        df = pd.DataFrame(params)
        db.update_batch_from_dataframe(schema_table=table_name, df=df)

        res = db.get_sql_dataframe(f'select * from {table_name}')

        # Make sure indexes from random_list have been changed
        self.assertEqual(len(randomlist), len(res[res.id.isin(randomlist)]))

        # Make sure changed indexes contain appropriate values
        self.assertEqual(len(randomlist),
                         res[res.id.isin(randomlist)]['color_name'].apply(lambda x: 1 if 'red' in x else 0).sum())

        # Make sure indexes from ~random_list have not been changed
        self.assertEqual(num_rows - len(randomlist), len(res[~res.id.isin(randomlist)]))

    @staticmethod
    def create_single_row_procedural_data(num_rows):
        values = []
        for i in range(num_rows):
            random_int_val = str(random.randint(0, 2147483647))
            values.append(random_int_val)
        return values

    # def test__insert_batches(self):
    #     db = self._get_db_inst()
    #     table_name = self._prepare_test_table(db)
    #
    #     # Test normal function
    #     sql = "insert into public.color (color_name) values %s;"
    #     params = [['ivory'], ['lemon'], ['copper'], ['salmon'], ['rust'], ['amber'], ['cream'], ['tan'], ['bronze'],
    #               ['blue'], ['silver'], ['grey']]
    #     db.insert_batches(sql, params)
    #
    #     res = db.get_sql_single_item_list(f'select color_name from {table_name}')
    #
    #     golden = 'grey'
    #     test = res[len(res)-1]
    #     self.assertEqual(golden, test)
    #
    #     # Test errors - Bad column name
    #     sql = 'insert into public.color (column_that_doesnt_exist) values %s;'
    #     failed = False
    #     try:
    #         db.insert_batches(sql, params)
    #     except Exception as e:
    #         failed = True
    #     self.assertTrue(failed)
    #
    #     # Syntax error (intos vs into)
    #     sql = 'insert intos public.color (color_name) values %s;'
    #     failed = False
    #     try:
    #         db.insert_batches(sql, params)
    #     except Exception as e:
    #         failed = True
    #     self.assertTrue(failed)

    def test__fix_missing_parenthesis(self):
        db = self._get_db_inst()

        # Without parenthesis.
        sql = 'update db.table (name) values %s'
        test = db._fix_missing_parenthesis(sql)
        golden = 'update db.table (name) values (%s)'
        self.assertEqual(golden, test)

        # With parenthesis.
        sql = 'update db.table (name) values (%s)'
        test = db._fix_missing_parenthesis(sql)
        golden = 'update db.table (name) values (%s)'
        self.assertEqual(golden, test)

    def test__build_sql_from_dataframe(self):
        db = self._get_db_inst()
        df = pd.DataFrame({'color_name': ['ivory', 'lemon', 'copper', 'salmon', 'rust', 'amber', 'cream', 'tan',
                                          'bronze', 'blue', 'silver', 'grey']})

        sql, params = db.build_sql_from_dataframe(df, 'color', 'public')
        golden = 'insert into public.color (color_name) values %s;'
        self.assertEqual(golden, sql)

        golden = ['ivory']
        self.assertEqual(golden, params[0])

    # def test__update_db_(self):
    #     db = self._get_db_inst()
    #     table_name = self._prepare_test_table(db)
    #
    #     # test to determine if replacing string works
    #     res = db.get_sql_dataframe(f'select * from {table_name}')
    #     res.loc[5,'color_name'] = 'orenge'
    #     db.update_db(res, update_cols=['color_name'], static_cols=['id'], schema='public', table='color')
    #     res = db.get_sql_dataframe(f'select * from {table_name}')
    #     self.assertTrue(res.loc[5,'color_name'] == 'orenge')
    #
    #     # test to determine if replacing int works
    #     res = db.get_sql_dataframe(f'select * from {table_name}')
    #     res.loc[5,'id'] = 10
    #     db.update_db(res, update_cols=['id'], static_cols=['color_name'], schema='public', table='color')
    #     res = db.get_sql_dataframe(f'select * from {table_name}')
    #     self.assertTrue(res.loc[5,'id'] == 10)
    #
    #     # test to determine if replacing float works
    #     res = db.get_sql_dataframe(f'select * from {table_name}')
    #     res.loc[5,'id'] = 10.0
    #     db.update_db(res, update_cols=['id'], static_cols=['color_name'], schema='public', table='color')
    #     res = db.get_sql_dataframe(f'select * from {table_name}')
    #     self.assertTrue(res.loc[5,'id'] == 10.0)
