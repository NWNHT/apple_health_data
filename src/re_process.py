
import logging
import pandas as pd
import re
import time
import xml.etree.ElementTree as et

from DBConn import DBConn

logger = logging.getLogger('__main__.' + __name__)

class reProcess:
    # Note that this leaves out the HKMetadatKeyHeartRateMotionContext metadata
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.db = None
    
    def parse(self):

        # <Record type="..." ... unit="..." ... creationDate="..." startDate="..." endDate="..." value="..."/>

        patt = re.compile(r'<Record type=\"(.*?)\" .* unit=\"(.*)\" creationDate=\"(.*)\" startDate=\"(.*)\" endDate=\"(.*)\" value=\"(.*)\">')

        with open(file=self.filename, mode='r') as fh:
            result = patt.findall(fh.read())

        return result

class xmlprocess:
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


if __name__ == '__main__':


    # reprocess = reProcess(filename='./../data/raw_data/export-2022-11-22.xml')
    xmlprocess = xmlprocess(filename='./../data/raw_data/export-2022-11-22.xml')

    start = time.perf_counter()
    result = xmlprocess.root
    print(time.perf_counter() - start)

    print(len(result))

    print(result[0])
