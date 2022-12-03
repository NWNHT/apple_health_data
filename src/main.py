
import logging
import pandas as pd
import plotnine as gg
import time

from DBConn import DBConn
from AppleHealthDB import AppleHealthDB
from raw_data_process import create_database, add_sleep_data

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
    db: AppleHealthDB = create_database('export-2022-11-27', 'AutoSleep-19991106-to-20221203.csv')
    # db: AppleHealthDB = create_database('partial', 'AutoSleep-19991106-to-20221203.csv')
    
    # add_sleep_data(db, 'AutoSleep-19991106-to-20221203.csv')
    
    heartrate = "HKQuantityTypeIdentifierHeartRate"
    resting_heartrate = "HKQuantityTypeIdentifierRestingHeartRate"
    flights = "HKQuantityTypeIdentifierFlightsClimbed"
    steps = "HKQuantityTypeIdentifierStepCount"

    # df = pd.DataFrame(db.record_by_date_range(flights, '2022-08-10', '2022-08-20').fetchall(), columns=['id', 'record_type', 'unit', 'creation_date', 'start_date', 'end_date', 'value']).set_index('id')
    df = pd.DataFrame(db.sum_by_date_range(steps, start_date='2018-10-01').fetchall(), columns=['date', 'value']).set_index('date')
    df['value'] = df['value'].apply(lambda x: 30000 if x > 50000 else x)
    # print(df)

    g = gg.ggplot(df, gg.aes(x=df.index, y='value')) + gg.geom_col(fill='#ff4f00') + gg.scale_x_datetime(date_breaks="5 months", expand=(0,0))
    f = gg.ggplot(df, gg.aes(x='value')) + gg.geom_histogram(binwidth=1000, fill='#ff4f00')
    # print(f)

    sql_query = """
    SELECT SUBSTR(d.starting_date, 0, 5) as year,
           SUBSTR(d.starting_date, 6, 5) as day,
           SUM(record_value) as value
    FROM Data d
    JOIN RecordType r
    ON d.record_type_id = r.record_type_id
    JOIN UnitType u
    ON d.unit_id = u.unit_id
    WHERE r.record_type = ?
      AND d.starting_date BETWEEN DATE(?) AND DATE(?)
    GROUP BY 1, 2
    """

    df2 = pd.DataFrame(db.execute_query(sql_query, (flights, '2018-10-01', db.last_date)).fetchall(), columns=['year', 'day', 'value'])
    f = (gg.ggplot(df2, gg.aes(x='value')) 
       + gg.geom_histogram(binwidth=1, fill='#ff4f00', colour='black')
       + gg.facet_grid('year ~ .'))
    print(f)


    
