from sqlalchemy import create_engine, Table
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql.schema import MetaData
import datetime
from copy import deepcopy


class MigrateManager(object):
    def __init__(self, uri1, uri2):
        self.uri1 = uri1
        self.uri2 = uri2
        self.engine1, self.metadata1, self.session1 = self.get_sqla_objs(uri1)
        self.engine2, self.metadata2, self.session2 = self.get_sqla_objs(uri2)

    @staticmethod
    def get_sqla_objs(uri):
        engine = create_engine(uri)
        metadata = MetaData(bind=engine)
        session = sessionmaker(bind=engine)()
        return engine, metadata, session

    @staticmethod
    def to_json(result_obj):
        """将sqlalchemy 数据对象转化成json"""
        # TODO 忽略了时间字段
        result = dict()
        if getattr(result_obj, '__mapper__'):
            for key in result_obj.__mapper__.c.keys():
                col = getattr(result_obj, key)
                if not isinstance(col, datetime.datetime) or isinstance(col, datetime.date):
                    result[key] = col
        elif getattr(result_obj, 'keys'):
            for key in result_obj.keys():
                col = getattr(result_obj, key)
                if not isinstance(col, datetime.datetime) or isinstance(col, datetime.date):
                    result[key] = col
        assert not result, "invalid result object"
        return result

    @staticmethod
    def get_table(table_name, engine, schema=None):
        return Table(table_name,
                     MetaData(bind=engine),
                     schema=schema or None,
                     autoload=True,
                     autoload_with=engine)

    def parse_table_tree(self, table_tree):



    def migrate(self, table_tree=None):
        for table in tables:
            if not session2.query(sqla_table).filter(sqla_table.c.unique_name == table.unique_name).first():
                # 读数据
                table_info = to_json(table)
                if table_info['database_id'] == 4:
                    table_info['database_id'] = 2
                table_info['sql_text'] = table_info['sql']
                del table_info['sql']
                table_info['metrics'] = []
                table_info['columns'] = []
                table_info['parameters'] = []
                table_info['filters'] = []

                metrics = session.query(table_metric).filter(table_metric.c.table_id == table_info['id']).all()
                for metric in metrics:
                    metric_info = metric.to_json()
                    table_info['metrics'].append(metric_info)

                parameters = session.query(table_parameter).filter(table_parameter.c.table_id == table_info['id']).all()
                for parameter in parameters:
                    parameter_info = parameter.to_json()
                    table_info['parameters'].append(parameter_info)

                columns = session.query(table_column).filter(table_column.c.table_id == table_info['id']).all()
                for column in columns:
                    column_info = column.to_json()
                    table_info['columns'].append(column_info)

                filters = session.query(table_filter).filter(table_filter.c.table_id == table_info['id']).all()
                for flt in filters:
                    flt_info = flt.to_json()
                    table_info['filters'].append(flt_info)

                # 迁移数据
                success_sql_file = 'sql/sql{}.sql'.format(table.id)
                # error_sql_file = 'sql/error/sql{}.sql'.format(table.id)
                try:
                    # sqla_table
                    info = table.to_json()
                    info['sql_text'] = info['sql']
                    del info['sql']
                    in_sql = get_insert_sql(sqla_table.name, info, auto_increment=False)
                    engine2.execute(in_sql)
                    # table_columns
                    for column in table_info['columns']:
                        in_sql = get_insert_sql(table_column.name, column)
                        engine2.execute(in_sql)
                    # table_metrics
                    for metric in table_info['metrics']:
                        in_sql = get_insert_sql(table_metric.name, metric)
                        engine2.execute(in_sql)
                    # table_parameters
                    for parameter in table_info['parameters']:
                        in_sql = get_insert_sql(table_parameter.name, parameter)
                        engine2.execute(in_sql)
                    # table_filters
                    for flt in table_info['filters']:
                        in_sql = get_insert_sql(table_filter.name, flt)
                        engine2.execute(in_sql)
                except Exception:
                    print('ERROR : \n table_id:{}\n error_sql:{}\n'.format(table.id, in_sql))
                    engine2.execute('delete from "PLATFORM"."TABLE_FILTER" where "TABLE_ID" = {}'.format(table.id))
                    engine2.execute('delete from "PLATFORM"."TABLE_PARAMETERS" where "TABLE_ID" = {}'.format(table.id))
                    engine2.execute('delete from "PLATFORM"."SQL_METRICS" where "TABLE_ID" = {}'.format(table.id))
                    engine2.execute('delete from "PLATFORM"."TABLE_COLUMNS" where "TABLE_ID" = {}'.format(table.id))
                    engine2.execute('delete from "PLATFORM"."SQLA_TABLES" where "ID" = {}'.format(table.id))


def to_json(result_obj):
    """将sqlalchemy 数据对象转化成json"""
    # TODO 忽略了时间字段
    result = dict()
    if getattr(result_obj, '__mapper__'):
        for key in result_obj.__mapper__.c.keys():
            col = getattr(result_obj, key)
            if not isinstance(col, datetime.datetime) or isinstance(col, datetime.date):
                result[key] = col
    elif getattr(result_obj, 'keys'):
        for key in result_obj.keys():
            col = getattr(result_obj, key)
            if not isinstance(col, datetime.datetime) or isinstance(col, datetime.date):
                result[key] = col
    assert not result, "invalid result object"
    return result


def get_table(table_name, engine, schema=None):
    return Table(table_name,
                 MetaData(bind=engine),
                 schema=schema or None,
                 autoload=True,
                 autoload_with=engine)


def get_insert_sql(table_name, insert_json, auto_increment=True):
    insert_json = deepcopy(insert_json)
    in_sql = 'INSERT INTO {}({}) VALUES ({})'
    del_keys = []
    for key, value in insert_json.items():
        if value is None:
            del_keys.append(key)
        elif isinstance(value, str):
            insert_json[key] = "'" + value + "'"
        elif isinstance(value, bool):
            # 让bool支持TinyInt
            insert_json[key] = str(int(value))
        elif isinstance(value, int):
            insert_json[key] = str(value)
        if key is 'id' and auto_increment:
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


def get_sqla_objs(uri):
    engine = create_engine(uri)
    metadata = MetaData(bind=engine)
    session = sessionmaker(bind=engine)()
    return engine, metadata, session


# 'hana://SCI_APPLICATION:SCIsci123@10.96.81.180:30059'
# 'hana+hdbcli://lidm1:lidm1(SCI)@10.122.13.22:30353'

uri = 'hana+hdbcli://lidm1:lidm1(SCI)@10.122.13.22:30353'
engine, metadata, session = get_sqla_objs(uri)
uri2 = 'hana://SCI_APPLICATION:SCIsci123@10.96.81.180:30059'
engine2, metadata2, session2 = get_sqla_objs(uri)

sqla_table = get_table('SQLA_TABLES', engine, schema='PLATFORM')
table_parameter = get_table('TABLE_PARAMETERS', engine, schema='PLATFORM')
table_filter = get_table('TABLE_FILTER', engine, schema='PLATFORM')
table_metric = get_table('SQL_METRICS', engine, schema='PLATFORM')
table_column = get_table('TABLE_COLUMNS', engine, schema='PLATFORM')

tables = session.query(sqla_table).all()

data = []
sql = []

for table in tables:
    if not session2.query(sqla_table).filter(sqla_table.c.unique_name == table.unique_name).first():
        # 读数据
        table_info = to_json(table)
        if table_info['database_id'] == 4:
            table_info['database_id'] = 2
        table_info['sql_text'] = table_info['sql']
        del table_info['sql']
        table_info['metrics'] = []
        table_info['columns'] = []
        table_info['parameters'] = []
        table_info['filters'] = []

        metrics = session.query(table_metric).filter(table_metric.c.table_id == table_info['id']).all()
        for metric in metrics:
            metric_info = metric.to_json()
            table_info['metrics'].append(metric_info)

        parameters = session.query(table_parameter).filter(table_parameter.c.table_id == table_info['id']).all()
        for parameter in parameters:
            parameter_info = parameter.to_json()
            table_info['parameters'].append(parameter_info)

        columns = session.query(table_column).filter(table_column.c.table_id == table_info['id']).all()
        for column in columns:
            column_info = column.to_json()
            table_info['columns'].append(column_info)

        filters = session.query(table_filter).filter(table_filter.c.table_id == table_info['id']).all()
        for flt in filters:
            flt_info = flt.to_json()
            table_info['filters'].append(flt_info)

        # 迁移数据
        success_sql_file = 'sql/sql{}.sql'.format(table.id)
        # error_sql_file = 'sql/error/sql{}.sql'.format(table.id)
        try:
            # sqla_table
            info = table.to_json()
            info['sql_text'] = info['sql']
            del info['sql']
            in_sql = get_insert_sql(sqla_table.name, info, auto_increment=False)
            engine2.execute(in_sql)
            # table_columns
            for column in table_info['columns']:
                in_sql = get_insert_sql(table_column.name, column)
                engine2.execute(in_sql)
            # table_metrics
            for metric in table_info['metrics']:
                in_sql = get_insert_sql(table_metric.name, metric)
                engine2.execute(in_sql)
            # table_parameters
            for parameter in table_info['parameters']:
                in_sql = get_insert_sql(table_parameter.name, parameter)
                engine2.execute(in_sql)
            # table_filters
            for flt in table_info['filters']:
                in_sql = get_insert_sql(table_filter.name, flt)
                engine2.execute(in_sql)
        except Exception:
            print('ERROR : \n table_id:{}\n error_sql:{}\n'.format(table.id, in_sql))
            engine2.execute('delete from "PLATFORM"."TABLE_FILTER" where "TABLE_ID" = {}'.format(table.id))
            engine2.execute('delete from "PLATFORM"."TABLE_PARAMETERS" where "TABLE_ID" = {}'.format(table.id))
            engine2.execute('delete from "PLATFORM"."SQL_METRICS" where "TABLE_ID" = {}'.format(table.id))
            engine2.execute('delete from "PLATFORM"."TABLE_COLUMNS" where "TABLE_ID" = {}'.format(table.id))
            engine2.execute('delete from "PLATFORM"."SQLA_TABLES" where "ID" = {}'.format(table.id))
