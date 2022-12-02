
from functools import cached_property

from DBConn import DBConn

class AppleHealthDB(DBConn):
    def __init__(self, db_name: str):
        super().__init__(db_name=db_name, filepath="./../data/")
    
    @cached_property
    def record_types(self):
        sql_command = """SELECT record_type FROM RecordType"""
        self.execute_query(sql_command)

        return [x[0] for x in self.cursor.fetchall()]
    
    @cached_property
    def unit_types(self):
        sql_command = """SELECT unit FROM UnitType"""
        self.execute_query(sql_command)

        return [x[0] for x in self.cursor.fetchall()]

