import logging
from multiprocessing import Pool
from os.path import isfile
import pandas as pd
import sqlite3
import time
from typing import List, Optional

logger = logging.getLogger('__main__.' + __name__)

class DBConn:
	instance = None

	def __new__(cls, *args, **kwargs):
		if cls.instance == None:
			cls.instance = super().__new__(DBConn)
		return cls.instance
	
	def __init__(self, db_name: str):
		self.name = db_name
		self.conn = self.connect()
		self.cursor = self.conn.cursor()
		self.data_path = './../data/'
	
	def connect(self):
		"""
		Check if database exists and return cursor, if no database then create one and initialize with script.
		"""
		
		# Create db and make tables if it does not exist
		if not isfile(self.data_path + self.name):
			try:
				self.conn = sqlite3.connect(self.name)
				self.cursor = self.conn.cursor()
				logger.info('No existing database, creating database.')
				self.create_tables()
				return self.conn
			except:
				logger.critical("Error creating database.")
				quit()
		else:
			try:
				logger.info("Connecting to database.")
				return sqlite3.connect(self.data_path + self.name)
			except sqlite3.Error as e:
				logger.critical("Error connecting to database.")
				quit()
	
	def __del__(self):
		self.cursor.close()
		self.conn.close()
	
	def commit(self):
		"""
		Perform commit on database
		"""

		logger.info("Committing to database.")
		self.conn.commit()

	def drop_tables(self):
		"""
		Drop all chess database tables
		"""

		with open('./SQLite_scripts/drop_tables.sql', 'r') as fh:
			commands = fh.read()

		logger.info("Dropping all tables.")
		self.cursor.executescript(commands)
		self.commit()

	def create_tables(self):
		"""
		Create all chess database tables
		"""

		with open('./SQLite_scripts/create_tables.sql', 'r') as fh:
			commands = fh.read()
		
		logger.info("Creating all tables.")
		self.cursor.executescript(commands)
		self.commit()

	def execute_command(self, command: str, arguments: Optional[tuple], commit: bool=True):
		"""
		Execute arbitrary command
		"""

		logger.debug(f"Executing command {command}.")
		if arguments is None:
			self.cursor.execute(command)
		else:
			self.cursor.execute(command, arguments)
		if commit: self.commit()

	def execute_query(self, query: str, arguments: Optional[tuple]=None):
		"""
		Execute arbitrary query
		"""

		logger.debug(f"Executing query {query}.")
		if arguments is None:
			return self.cursor.execute(query)
		else:
			return self.cursor.execute(query, arguments)

	def does_game_exist_by_id(self, game_id: int) -> bool:
		"""
		Return a bool indicating if a game is in the database
		"""

		sql_query = """SELECT DISTINCT game_id
					   FROM Game
					   WHERE game_id = ?"""
		
		resp = self.cursor.execute(sql_query, (game_id,)).fetchall()

		return bool(len(resp))

	def evaluate_game_by_id(self, game_id: int, parallel: bool=False, commit: bool=True):
		"""
		Evaluate all positions from the given game_id
		"""

		sql_read_command = """	SELECT p.position_id, p.fen
								FROM Game g
								JOIN GameMove gm
								ON g.game_id = gm.game_id
								JOIN Move m
								ON gm.move_id = m.move_id
								JOIN Position p
								ON m.position_id = p.position_id
								WHERE g.game_id = ?
								  AND (eval_depth < ? or eval_depth IS NULL)
								ORDER BY move_num"""
		sql_write_command = """UPDATE Position SET eval_depth=?, first_move=?, second_move=?, third_move=?, first_move_eval=?, second_move_eval=?, third_move_eval=?, first_move_eval_type=?, second_move_eval_type=?, third_move_eval_type=? WHERE position_id = ?"""

		# Request positions without an evaluation
		self.cursor.execute(sql_read_command, (game_id, self.sf_depth))
		resp = self.cursor.fetchall()
		logger.info(f"Evaluating {len(resp)} positions at depth {self.sf_depth}.")

		# Get the evaluations
		if parallel:
			evaluations = self.eval_positions_parallel(resp)
		else:
			evaluations = self.eval_positions(resp)

		logger.debug("Done evaluating positions.")

		# Write all of the evaluations to the database
		self.cursor.executemany(sql_write_command, evaluations)
		if commit: self.commit()


if __name__ == '__main__':

	start = time.perf_counter()

	db = DBConn('health_data.db')


	del(db)

	print(f"Total time: {time.perf_counter() - start}")
