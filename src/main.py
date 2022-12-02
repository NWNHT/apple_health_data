
import logging
import pandas as pd
import plotnine as gg

from DBConn import DBConn
from AppleHealthDB import AppleHealthDB
from raw_data_process import xml_to_sql

# class AppleHealthDB(DBConn):
#     def __init__(self, db_name: str):
#         super().__init__(db_name=db_name, filepath="./../data/")

# Logging  
# Create formatter and the two handlers
log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s - %(name)s')
f_handler = logging.FileHandler('log.log')
s_handler = logging.StreamHandler()

# Set the levels and formats of the handlers
f_handler.setLevel(logging.DEBUG)
s_handler.setLevel(logging.INFO)
f_handler.setFormatter(log_format)
s_handler.setFormatter(log_format)

# Get the logger
logger = logging.getLogger(__name__)   
logger.setLevel(logging.DEBUG)
logger.addHandler(f_handler)
logger.addHandler(s_handler)


if __name__ == '__main__':

    # Create the database from the XML file
    db: AppleHealthDB = xml_to_sql('export-2022-11-27')

    sql_query = """
    SELECT starting_date, record_value
    FROM Data d
    JOIN RecordType r
    ON d.record_type_id = r.record_type_id
    JOIN UnitType u
    ON d.unit_id = u.unit_id
    WHERE r.record_type = ?
      AND d.starting_date > date('2022-11-23')"""

    df = pd.DataFrame(db.execute_query(sql_query, ("HKQuantityTypeIdentifierHeartRate",)).fetchall(), columns=['time', 'heartrate']).set_index('time')

    g = gg.ggplot(df, gg.aes(x=df.index, y='heartrate')) + gg.geom_point()
    print(g)
