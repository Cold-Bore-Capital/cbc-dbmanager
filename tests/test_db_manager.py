# import psycopg2
import random
from unittest import TestCase
from datetime import datetime, date
import pandas as pd
import pytz
from cbcdb.main import DBManager, MissingDatabaseColumn, MissingDTypeFromTypes
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
                    another_value VARCHAR,
                    an_int integer,
                    a_date date,
                    a_timestamp timestamp,
                    a_number numeric(6,2),
                    a_big_int bigint,
                    a_small_int smallint
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

    def test__update_batch_from_df(self):
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
        db.update_batch_from_df(df, ['color_name', 'another_value'], ['id'], 'public', 'color')

        res = db.get_sql_dataframe(f'select * from {table_name}')

        # Make sure indexes from random_list have been changed
        self.assertEqual(len(randomlist), len(res[res.id.isin(randomlist)]))

        # Make sure changed indexes contain appropriate values
        self.assertEqual(len(randomlist),
                         res[res.id.isin(randomlist)]['color_name'].apply(lambda x: 1 if 'red' in x else 0).sum())

        # Make sure indexes from ~random_list have not been changed
        self.assertEqual(num_rows - len(randomlist), len(res[~res.id.isin(randomlist)]))

    def test__set_column_value(self):
        db = self._get_db_inst()

        quote_flag_dict = {'c_string': 1, 'c_number': 0, 'c_null': 1, 'c_datetime': 1, 'c_date':1, 'c_pd_timestamp': 1,
                           'c_tz_aware': 1, 'c_bool': 0}

        test_cases = [
            {'col': 'c_string', 'val': 'abc', 'golden': "c_string='abc', "}, # String
            {'col': 'c_number', 'val': 1, 'golden': "c_number=1, "}, # Number
            {'col': 'c_null', 'val': None, 'golden': "c_null=null, "}, # Null
            {'col': 'c_datetime', 'val': datetime(2021,1,1,12,0,0),
             'golden': "c_datetime='2021-01-01T12:00:00', "}, # datetime
            {'col': 'c_date', 'val': date(2021, 1, 1),
             'golden': "c_date='2021-01-01', "},  # datetime
            {'col': 'c_pd_timestamp', 'val': pd.Timestamp(2021,1,1,12,0,0),
             'golden': "c_pd_timestamp='2021-01-01T12:00:00', "}, # pandas timestamp
            {'col': 'c_tz_aware', 'val': datetime(2021, 1, 1, 12, 0, 0, tzinfo=pytz.utc),
             'golden': "c_tz_aware='2021-01-01T12:00:00+0000', "}, # Timezone aware
            {'col': 'c_bool', 'val': True, 'golden': "c_bool=1, "},
            {'col': 'c_bool', 'val': False, 'golden': "c_bool=0, "},

        ]

        for test_case in test_cases:
            col = test_case['col']
            val = test_case['val']
            golden = test_case['golden']
            test = db._set_column_value(col, val, ',', quote_flag_dict)
            self.assertEqual(golden, test)

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

    def test__get_table_column_dtypes(self):
        db = self._get_db_inst()
        self._prepare_test_table(db)

        # Test working
        test = db._get_table_column_dtypes('public', 'color', ['color_name', 'another_value', 'an_int', 'a_date',
                                                               'a_timestamp', 'a_number', 'a_big_int', 'a_small_int'])
        golden = {'color_name': 1, 'another_value': 1, 'an_int': 0, 'a_date': 1, 'a_timestamp': 1, 'a_number': 0,
                  'a_big_int': 0, 'a_small_int': 0}
        self.assertDictEqual(golden, test)

        # Test missing column name
        with self.assertRaises(MissingDatabaseColumn):
            test = db._get_table_column_dtypes('public', 'color',
                                               ['a_bad_column_name', 'another_value', 'an_int', 'a_date',
                                                'a_timestamp', 'a_number', 'a_big_int', 'a_small_int'])

        # Test finding an unknown dtype
        with self.assertRaises(MissingDTypeFromTypes):
            db = MockMissingDTypeFromTypes(debug_output_mode=True,
                                           use_ssh=False,
                                           ssh_remote_bind_port=5434,
                                           db_name='test',
                                           db_user='test',
                                           db_password='test',
                                           db_schema='public',
                                           db_host='localhost')

            test = db._get_table_column_dtypes('public', 'color',
                                               ['color_name', 'another_value', 'an_int', 'a_date',
                                                'a_timestamp', 'a_number', 'a_big_int', 'a_small_int'])

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


class MockMissingDTypeFromTypes(DBManager):

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
        super().__init__(debug_output_mode,
                         use_ssh,
                         ssh_key_path,
                         ssh_host,
                         ssh_port,
                         ssh_user,
                         ssh_remote_bind_address,
                         ssh_remote_bind_port,
                         ssh_local_bind_address,
                         ssh_local_bind_port,
                         db_name,
                         db_user,
                         db_password,
                         db_schema,
                         db_host)

    def get_sql_list_dicts(self, sql, params=None):
        val = [{'column_name': 'id', 'data_type': 'integer'},
               {'column_name': 'color_name', 'data_type': 'an unknown dtype'},  # This is the bad dtype
               {'column_name': 'another_value', 'data_type': 'character varying'},
               {'column_name': 'an_int', 'data_type': 'integer'}, {'column_name': 'a_date', 'data_type': 'date'},
               {'column_name': 'a_timestamp', 'data_type': 'timestamp without time zone'},
               {'column_name': 'a_number', 'data_type': 'numeric'}, {'column_name': 'a_big_int', 'data_type': 'bigint'},
               {'column_name': 'a_small_int', 'data_type': 'smallint'}]
        return val
