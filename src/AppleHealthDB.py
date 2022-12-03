
import datetime
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
    
    @cached_property
    def first_date(self):
        sql_command = """
        SELECT MIN(SUBSTR(ending_date, 0, 11))
        FROM Data
        """

        return self.execute_query(sql_command).fetchone()[0]
    
    @cached_property
    def last_date(self):
        sql_command = """
        SELECT MAX(SUBSTR(ending_date, 0, 11))
        FROM Data
        """

        return self.execute_query(sql_command).fetchone()[0]
    
    def record_by_date_range(self, record: str, start_date: str='', end_date: str=''):
        """
        Given record, starting and end dates, query for all records within date range and return cursor
        """

        if start_date == '': start_date = self.first_date
        if end_date == '': end_date = self.last_date
        
        sql_query = """
        SELECT record_id, r.record_type, u.unit, creation_date, starting_date, ending_date, record_value
        FROM Data d
        JOIN RecordType r
        ON d.record_type_id = r.record_type_id
        JOIN UnitType u
        ON d.unit_id = u.unit_id
        WHERE r.record_type = ?
          AND d.starting_date BETWEEN DATE(?) AND DATE(?)
        """
        
        return self.execute_query(sql_query, (record, start_date, end_date))
    
    def sum_by_date_range(self, record: str, start_date: str='', end_date: str=''):
        """
        Given record, starting and end dates, query for all records with date range and sum by day, return cursor"""

        if start_date == '': start_date = self.first_date
        if end_date == '': end_date = self.last_date

        sql_query = """
        SELECT SUBSTR(d.starting_date, 0, 11),
               SUM(record_value)
        FROM Data d
        JOIN RecordType r
        ON d.record_type_id = r.record_type_id
        JOIN UnitType u
        ON d.unit_id = u.unit_id
        WHERE r.record_type = ?
          AND d.starting_date BETWEEN DATE(?) AND DATE(?)
        GROUP BY 1
        """

        return self.execute_query(sql_query, (record, start_date, end_date))
