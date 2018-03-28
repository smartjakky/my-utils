from sqlalchemy import create_engine, Table
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql.schema import MetaData
from copy import deepcopy
import datetime
import json


class Migrater(object):
    def __init__(self, uri1, uri2, table_tree, schema=None):
        self.uri1 = 'hana+hdbcli://lidm1:lidm1(SCI)@10.122.13.22:30353'
        self.uri2 = 'hana://SCI_APPLICATION:SCIsci123@10.96.81.180:30059'
        self.engine1, self.metadata1, self.session1 = self.get_sqla_objs(uri1)
        self.engine2, self.metadata2, self.session2 = self.get_sqla_objs(uri2)
        self.table_tree = {'SQLA_TABLES': ['TABLE_PARAMETERS', 'TABLE_FILTER', 'SQL_METRICS', 'TABLE_COLUMNS']}
        self.schema = None
        self._table_list = []

    @staticmethod
    def to_json(result_obj):
        """将sqlalchemy 数据对象转化成json"""
        # TODO 忽略了时间字段
        result = dict()
        if getattr(result_obj, '__mapper__', None):
            for key in result_obj.__mapper__.c.keys():
                col = getattr(result_obj, key)
                if not (isinstance(col, datetime.datetime) and isinstance(col, datetime.date)):
                    result[key] = col
                else:
                    result[key] = col.strftime('%Y%m')
        elif getattr(result_obj, 'keys', None):
            for key in result_obj.keys():
                col = getattr(result_obj, key)
                if not (isinstance(col, datetime.datetime) and isinstance(col, datetime.date)):
                    result[key] = col
        assert result, "invalid result object"
        return result

    def get_table(self, table_name, engine=None, schema=None):
        if not engine:
            engine = self.engine1
        if not schema:
            schema = self.schema
        return Table(table_name, MetaData(bind=engine), schema=schema or None, autoload=True, autoload_with=engine)

    @staticmethod
    def get_sqla_objs(uri):
        engine = create_engine(uri)
        metadata = MetaData(bind=engine)
        session = sessionmaker(bind=engine)()
        return engine, metadata, session

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
                # 让bool支持TinyInt
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

    def _parse_tree(self, node):
        if isinstance(node, dict):
            for sub_node in node.keys():
                self.table_list.append(sub_node)
                self.parse_tree(node[sub_node])
        elif isinstance(node, list):
            for subling in node:
                self.parse_tree(subling)
        else:
            return

    def get_table_list(self):
        self._parse_tree()
        return self._table_list

    def delete_inserts_when_fail(self):
        if not self._table_list:
            self._parse_tree(self.table_tree)
        delete_sql = 'delete from {} where {} = {}'
        for table in self._table_list[::-1]:
            # TODO how to get foreign id field name
            self.engine2.execute(delete_sql.format(table.arg))

    def migrate(self):
        # sqla_table = self.get_table('SQLA_TABLES', engine, schema='PLATFORM')
        # table_parameter = get_table('TABLE_PARAMETERS', engine, schema='PLATFORM')
        # table_filter = get_table('TABLE_FILTER', engine, schema='PLATFORM')
        # table_metric = get_table('SQL_METRICS', engine, schema='PLATFORM')
        # table_column = get_table('TABLE_COLUMNS', engine, schema='PLATFORM')
        table_objects = [self.get_table(table_name, self.engine1) for table_name in self.get_table_list()]
        try:
            for table_object in table_objects:
                table_info = self.to_json(table_object)
                in_sql = self.get_insert_sql(table_object)
            # sqla_table
            # info = self.to_json(table)
            # if info['database_id'] == 4:
            #     info['database_id'] = 2
            # in_sql = self.get_insert_sql(sqla_table.name, info, auto_increment=False)
            # self.engine2.execute(in_sql)
            # # table_columns
            # for column in table_info['columns']:
            #     in_sql = self.get_insert_sql(table_column.name, column)
            #     self.engine2.execute(in_sql)
            # # table_metrics
            # for metric in table_info['metrics']:
            #     in_sql = self.get_insert_sql(table_metric.name, metric)
            #     self.engine2.execute(in_sql)
            # # table_parameters
            # for parameter in table_info['parameters']:
            #     in_sql = self.get_insert_sql(table_parameter.name, parameter)
            #     self.engine2.execute(in_sql)
            # # table_filters
            # for flt in table_info['filters']:
            #     in_sql = self.get_insert_sql(table_filter.name, flt)
            #     self.engine2.execute(in_sql)
        except Exception:
            print('ERROR : \n table_id:{}\n error_sql:{}\n'.format(table.id, in_sql))
            # self.delete_inserts_when_fail({'id': root_table_id})
        # table = self.session1.query(sqla_table).filter(sqla_table.c.id == 219).first()
        # table_info = to_json(table)
        # table_info['metrics'] = []
        # table_info['columns'] = []
        # table_info['parameters'] = []
        # table_info['filters'] = []
        #
        # metrics = session.query(table_metric).filter(table_metric.c.table_id == table_info['id']).all()
        # for metric in metrics:
        #     metric_info = to_json(metric)
        #     table_info['metrics'].append(metric_info)
        #
        # parameters = session.query(table_parameter).filter(table_parameter.c.table_id == table_info['id']).all()
        # for parameter in parameters:
        #     parameter_info = to_json(parameter)
        #     table_info['parameters'].append(parameter_info)
        #
        # columns = session.query(table_column).filter(table_column.c.table_id == table_info['id']).all()
        # for column in columns:
        #     column_info = to_json(column)
        #     table_info['columns'].append(column_info)
        #
        # filters = session.query(table_filter).filter(table_filter.c.table_id == table_info['id']).all()
        # for flt in filters:
        #     flt_info = to_json(flt)
        #     table_info['filters'].append(flt_info)
        # with open('sql/data.json', 'w') as f:
        #     f.write(json.dumps(table_info, indent=2))
        # 迁移数据
        # success_sql_file = 'sql/sql{}.sql'.format(table.id)
        # error_sql_file = 'sql/error/sql{}.sql'.format(table.id)

