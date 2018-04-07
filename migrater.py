from sqlalchemy import create_engine, Table
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql.schema import MetaData
from copy import deepcopy
import datetime


class Migrater(object):
    """迁移表数据的类"""

    def __init__(self, unique_names,
                 uri1='hana+hdbcli://lidm1:lidm1(SCI)@10.122.13.22:30353',
                 uri2='hana://SCI_APPLICATION:SCIsci123@10.96.81.180:30059',
                 schema='PLATFORM'):
        self.uri1 = uri1
        self.uri2 = uri2
        self.engine1, self.metadata1, self.session1 = self.get_sqla_objs(uri1)
        self.engine2, self.metadata2, self.session2 = self.get_sqla_objs(uri2)
        self.unique_names = unique_names
        self.schema = schema

    @staticmethod
    def to_json(result_obj):
        """将sqlalchemy 数据对象转化成json"""
        result = dict()
        if getattr(result_obj, '__mapper__', None):
            for key in result_obj.__mapper__.c.keys():
                col = getattr(result_obj, key)
                if isinstance(col, datetime.datetime) or isinstance(col, datetime.date):
                    col = col.isoformat()
                result[key] = col
        elif getattr(result_obj, 'keys', None):
            for key in result_obj.keys():
                col = getattr(result_obj, key)
                if isinstance(col, datetime.datetime) or isinstance(col, datetime.date):
                    col = col.isoformat()
                result[key] = col
        assert result, "invalid result object"
        return result

    @staticmethod
    def get_sqla_objs(uri):
        engine = create_engine(uri)
        metadata = MetaData(bind=engine)
        session = sessionmaker(bind=engine)()
        return engine, metadata, session

    def get_table(self, table_name, engine=None, schema=None):
        if not engine:
            engine = self.engine1
        if not schema:
            schema = self.schema
        return Table(table_name, MetaData(bind=engine), schema=schema or None, autoload=True, autoload_with=engine)

    def get_insert_sql(self, table_name, insert_json, auto_increment=True):
        insert_json = deepcopy(insert_json)
        in_sql = 'INSERT INTO {}({}) VALUES ({})'
        del_keys = []
        for key, value in insert_json.items():
            if value is None:
                del_keys.append(key)
            elif isinstance(value, str):
                insert_json[key] = "'" + value + "'"
            elif isinstance(value, bool):
                insert_json[key] = str(int(value))
            elif isinstance(value, int):
                insert_json[key] = str(value)
            if key.lower() == 'id' and auto_increment:
                sequence = "\"PLATFORM\".\"{}".format(table_name.upper() + "_ID_SEQ\".NEXTVAL")
                insert_json[key] = sequence
        for key in del_keys:
            del insert_json[key]
        columns = insert_json.keys()
        columns = ['"' + column.upper() + '"' for column in columns]
        values = insert_json.values()
        table_name = "\"PLATFORM\".\"{}\"".format(table_name.upper())
        columns = ','.join(columns)
        values = ','.join(values)
        in_sql = in_sql.format(table_name, columns, values)
        return in_sql

    def migrate(self, file=None):
        """迁移数据"""

        sqla_table1 = self.get_table('SQLA_TABLES', engine=self.engine1)
        table_parameter1 = self.get_table('TABLE_PARAMETERS', engine=self.engine1)
        table_filter1 = self.get_table('TABLE_FILTER', engine=self.engine1)
        table_metric1 = self.get_table('SQL_METRICS', engine=self.engine1)
        table_column1 = self.get_table('TABLE_COLUMNS', engine=self.engine1)

        tables1 = self.session1.query(sqla_table1).filter(sqla_table1.c.unique_name.in_(self.unique_names)).all()

        all_table_json = []

        try:
            for table in tables1:
                # table sql
                sqls = []
                # table json
                table_json = self.to_json(table)
                metric_json = []
                column_json = []
                filter_json = []
                param_json = []
                params = self.session1.query(table_parameter1).filter(table_parameter1.c.table_id == table.id).all()
                metrics = self.session1.query(table_metric1).filter(table_metric1.c.table_id == table.id).all()
                columns = self.session1.query(table_column1).filter(table_column1.c.table_id == table.id).all()
                filters = self.session1.query(table_filter1).filter(table_filter1.c.table_id == table.id).all()
                for param in params:
                    param_json.append(self.to_json(param))
                for metric in metrics:
                    metric_json.append(self.to_json(metric))
                for column in columns:
                    column_json.append(self.to_json(column))
                for flt in filters:
                    filter_json.append(self.to_json(flt))
                table_json_clone = deepcopy(table_json)
                table_json_clone['metrics'] = metric_json
                table_json_clone['columns'] = column_json
                table_json_clone['filters'] = filter_json
                table_json_clone['params'] = param_json
                all_table_json.append(table_json_clone)
                # delete table info
                self.engine2.execute('delete from "PLATFORM"."TABLE_FILTER" where "TABLE_ID" = {}'.format(table.id))
                self.engine2.execute('delete from "PLATFORM"."TABLE_PARAMETERS" where "TABLE_ID" = {}'.format(table.id))
                self.engine2.execute('delete from "PLATFORM"."SQL_METRICS" where "TABLE_ID" = {}'.format(table.id))
                self.engine2.execute('delete from "PLATFORM"."TABLE_COLUMNS" where "TABLE_ID" = {}'.format(table.id))
                self.engine2.execute('delete from "PLATFORM"."SQLA_TABLES" where "ID" = {}'.format(table.id))
                # get insert SQL
                # tables
                sqls.append(self.get_insert_sql('SQLA_TABLES', table_json, auto_increment=False))
                # table_columns
                for column in column_json:
                    sqls.append(self.get_insert_sql('TABLE_COLUMNS', column))
                # table_metrics
                for metric in metric_json:
                    sqls.append(self.get_insert_sql('SQL_METRICS', metric))
                # table_parameters
                for parameter in param_json:
                    sqls.append(self.get_insert_sql('TABLE_PARAMETERS', parameter))
                # table_filters
                for flt in filter_json:
                    sqls.append(self.get_insert_sql('TABLE_FILTER', flt))
                # execute insert sql
                current_sql = None
                for sql in sqls:
                    current_sql = sql
                    self.engine2.execute(sql)
        except Exception:
            # if error delete inserted info and print error SQL
            self.engine2.execute('delete from "PLATFORM"."TABLE_FILTER" where "TABLE_ID" = {}'.format(table.id))
            self.engine2.execute('delete from "PLATFORM"."TABLE_PARAMETERS" where "TABLE_ID" = {}'.format(table.id))
            self.engine2.execute('delete from "PLATFORM"."SQL_METRICS" where "TABLE_ID" = {}'.format(table.id))
            self.engine2.execute('delete from "PLATFORM"."TABLE_COLUMNS" where "TABLE_ID" = {}'.format(table.id))
            self.engine2.execute('delete from "PLATFORM"."SQLA_TABLES" where "ID" = {}'.format(table.id))
            raise Exception('error when execute sql: {}'.format(current_sql))
        else:
            if file:
                # TODO
                raise Exception('TO DO')
            return all_table_json
