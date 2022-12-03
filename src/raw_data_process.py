
import datetime
import logging
from os.path import isfile
import pandas as pd
import re
import time
from typing import Optional
import xml.etree.ElementTree as et

from AppleHealthDB import AppleHealthDB

logger = logging.getLogger('__main__.' + __name__)

class reProcess:
    # Note that this leaves out the HKMetadatKeyHeartRateMotionContext metadata
    # This loses some data from the structure of the xml, who would have thought
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.db = None
    
    def parse(self):

        # <Record type="..." ... unit="..." ... creationDate="..." startDate="..." endDate="..." value="..."/>

        patt = re.compile(r'<Record type=\"(.*?)\" .* unit=\"(.*)\" creationDate=\"(.*)\" startDate=\"(.*)\" endDate=\"(.*)\" value=\"(.*)\">')

        with open(file=self.filename, mode='r') as fh:
            result = patt.findall(fh.read())

        return result


class XMLProcess:
    """Class to parse """
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.db = None
        self._tree = None
        self._root = None

    @property
    def tree(self):
        if self._tree is None:
            try:
                self._tree = et.parse(self.filename)
                logger.info('Successfully got the tree.')
            except Exception as e:
                logger.critical(f'Failed to get XML tree with exception: {e}')
                quit()
        return self._tree

    @property
    def root(self):
        if self._root is None:
            try:
                self._root = self.tree.getroot()
                logger.info('Successfully got the tree root.')
            except Exception as e:
                logger.critical(f'Failed to get XML root with exception: {e}')
                quit()
        return self._root


def add_to_database(root, batch_size: int=10000, filename: str='apple_health_data') -> AppleHealthDB:
    """Take the root of the xml tree and commit all records to the database"""
    db = AppleHealthDB(db_name=filename + '.db')

    create_record_type_command = """INSERT INTO RecordType (record_type) VALUES (?)"""
    create_unit_type_command = """INSERT INTO UnitType (unit) VALUES (?)"""
    create_record_command = """INSERT INTO Data (record_type_id, 
                                                 unit_id,
                                                 creation_date, 
                                                 starting_date, 
                                                 ending_date, 
                                                 record_value) 
                               VALUES ((SELECT record_type_id FROM RecordType WHERE record_type=?), 
                                       (SELECT unit_id FROM UnitType WHERE unit=?), ?, ?, ?, ?)"""

    batch = []
    record_type_cache = []
    unit_type_cache = []
    for i, record in enumerate(root.findall('Record'), start=1):

        record = record.attrib
        record_type = record.get('type', None)
        unit = record.get('unit', None)

        # Create the RecordType and UnitType tables as new types are found
        # This has cut like 25% of the database size
        if record_type not in record_type_cache:
            logger.debug(f"Adding {record_type} as a type.")
            db.execute_command(create_record_type_command, (record_type,))
            record_type_cache.append(record_type)

        if unit not in unit_type_cache:
            logger.debug(f"Adding {unit} as a unit.")
            db.execute_command(create_unit_type_command, (unit,))
            unit_type_cache.append(unit)

        try:
            batch.append((record_type, unit, record['creationDate'], record['startDate'], record['endDate'], record.get('value', None)))
        except Exception as e:
            logger.warning(f"Errored on entry {i}, skipping because of error: {e}")

        # Batch the commits
        if not i % batch_size:
            logger.debug(f"Execute many on i = {i}")
            db.execute_many(create_record_command, batch)
            batch = []
    else:
        # commit the final set not included in the last full batch
        if len(batch):
            logger.debug(f"Execute many on i = {i}")
            db.execute_many(create_record_command, batch)
    
    return db


def add_sleep_data(db: AppleHealthDB, filename: str):
    """Add Autosleep data to database

    Args:
        db (AppleHealthDB): _description_
        filename (str): _description_
    """

    # Read data
    logger.info(f"Reading AutoSleep data from file: {filename}")
    sleep_data = pd.read_csv('./../data/raw_data/' + filename)

    # Filter data
    filter_strings = ['Avg7', 'SpO2', 'tags', 'notes']
    keep_columns = [x for x in sleep_data.columns if all(y not in x for y in filter_strings)]
    sleep_data = sleep_data[keep_columns]
    sleep_data = clean_sleep_data(sleep_data)

    # Set up query
    columns = ['date', 'from_date', 'to_date', 'bedtime', 'waketime', 'in_bed', 'awake', 'fell_asleep_in', 'num_sessions', 'asleep', 'efficiency', 'quality', 'deep', 'sleep_BPM', 'day_BPM', 'waking_BPM', 'hrv', 'sleep_hrv', 'resp_avg', 'resp_min', 'resp_max']
    columns_string = ', '.join(columns)
    values_string = ', '.join(['?'] * len(columns))
    sql_query = f"""
    INSERT INTO Sleep ({columns_string})
    VALUES ({values_string})
    """

    # Execute query
    logger.debug(f"Adding AutoSleep data to SQLite db.")
    db.execute_many(sql_query, sleep_data.values.tolist())


def clean_sleep_data(df: pd.DataFrame):
    # Clean time_range to just include date, move back a day
    df['ISO8601'] = df['ISO8601'].apply(lambda x: (datetime.date.fromisoformat(x[:10]) - datetime.timedelta(days=1)).isoformat())
    
    return df


def create_database(filename: str='export-2022-11-27', auto_sleep_data_filename: Optional[str] = None) -> AppleHealthDB:
    """Given xml file name(sans .xml), create a SQLite database of the same name"""

    if isfile(f"./../data/{filename}.db"):
        logger.info(f"SQLite database already exists for file {filename}.")
        return AppleHealthDB(db_name=filename + '.db')
    elif not isfile(f"./../data/raw_data/{filename}.xml"):
        logger.critical(f"File {filename}.xml does not exist.")
        quit()
    else:
        logger.info(f"File {filename}.xml found but no database exists, creating database.")

    # Create xml processing object
    xmlprocess = XMLProcess(filename='./../data/raw_data/' + filename + '.xml')

    # Parse the xml
    start = time.perf_counter()
    result = xmlprocess.root
    logger.info(f"Total process time: {time.perf_counter() - start}")

    # Read the xml and commit to sqlite database
    start = time.perf_counter()
    db = add_to_database(result, filename=filename)
    logger.info(f"Total database time: {time.perf_counter() - start}")

    start = time.perf_counter()
    if auto_sleep_data_filename is not None:
        add_sleep_data(db, auto_sleep_data_filename)
    logger.info(f"Total sleep data time: {time.perf_counter() - start}")

    return db

        
if __name__ == '__main__':

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

    # Create xml processing object
    xmlprocess = XMLProcess(filename='./../data/raw_data/export-2022-11-27.xml')

    # Parse the xml
    start = time.perf_counter()
    result = xmlprocess.root
    logger.info(f"Total process time: {time.perf_counter() - start}")

    # Read the xml and commit to sqlite database
    start = time.perf_counter()
    add_to_database(result)
    logger.info(f"Total database time: {time.perf_counter() - start}")
