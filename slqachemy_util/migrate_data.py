from sqlalchemy import create_engine, Column, String, Integer, ForeignKey
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import MetaData
from sqlalchemy.ext.declarative import declarative_base


# 用于测试的参数
# ----------------------------------------------------------------------------
# uri = 'sqlite:///c:\\Users\\jakky\\Desktop\\import_demo\\demo.db'
# engine = create_engine(uri)
# metadata = MetaData(bind=engine)
# Model = declarative_base(bind=engine)
# session = sessionmaker(bind=engine)()
# db = engine
#
# uri2 = 'sqlite:///c:\\Users\\jakky\\Desktop\\import_demo\\main.db'
# engine2 = create_engine(uri2)
# metadata2 = MetaData(bind=engine2)
# Model2 = declarative_base(bind=engine2)
# session2 = sessionmaker(bind=engine2)()
# db2 = engine2


# class User(Model):
#     __tablename__ = 'user'
#     id = Column(Integer, primary_key=True)
#     name = Column(String, nullable=False)
#     roles = relationship('Role', secondary='user_role', backref='users')
#
#
# class Role(Model):
#     __tablename__ = 'role'
#     id = Column(Integer, primary_key=True)
#     name = Column(String, nullable=False)
#
#
# class UserRole(Model):
#     __tablename__ = 'user_role'
#     id = Column(Integer, primary_key=True)
#     user_id = Column(ForeignKey('user.id'))
#     role_id = Column(ForeignKey('role.id'))
#     user = relationship('User', backref="user_roles")
# ----------------------------------------------------------------------------


# 数据迁移工具
# ----------------------------------------------------------------------------
class MigrateManager(object):

    def __init__(self, from_db_uri, to_db_uri, migrate_tables, **kwargs):
        self.migrate_tables = migrate_tables

        self.from_db = create_engine(from_db_uri)
        self.from_metadata = MetaData(bind=self.from_db, reflect=True, schema=kwargs.get('from_schema', None))
        self.from_session = sessionmaker(bind=self.from_db)()

        self.to_db = create_engine(to_db_uri)
        self.to_metadata = MetaData(bind=self.to_db, schema=kwargs.get('to_schema', None))
        self.to_session = sessionmaker(bind=self.to_db)()

    def migrate_table(self):
        tables = dict()
        for table_name, table in self.from_metadata.tables.items():
            if table_name in self.migrate_tables:
                tables[table_name] = table
        self.to_metadata.tables = tables
        self.to_metadata.create_all(bind=self.to_db)

    def migrate_data(self):
        for table_name, table in self.to_metadata.tables.items():
            results = self.from_session.query(table).all()
            for result in results:
                self.to_db.execute(table.insert(result))
# ----------------------------------------------------------------------------
