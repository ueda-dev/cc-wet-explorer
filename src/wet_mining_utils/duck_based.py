from typing import List, NamedTuple
from io import BytesIO
import warcio
import duckdb
import gzip
import logging

class Record(NamedTuple):
    timestamp: str
    text: str

class ArchiveExtractor:
    def __init__(self, keywords: List[str]) -> None:
        self.sql = 'SELECT * FROM wet_data WHERE ' + ' OR '.join("content LIKE ?" for _ in keywords)
        self.keywords = [f'%{k}%' for k in keywords]
        self.con = self.initialize_duckdb()

    @classmethod
    def initialize_duckdb(self):
        conn = duckdb.connect(':memory:')
        conn.execute("""
            CREATE TABLE wet_data (timestamp TEXT, content TEXT)
        """)

        return conn
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def reset(self):
        self.con.execute('DELETE FROM wet_data')

    def close(self):
        self.con.close()

    def load_data(self, data:bytes):
        """
        data: CommonCrawlからダウンロードした生のcontent
        """

        try:
            with gzip.GzipFile(fileobj=BytesIO(data)) as f:
                for record in warcio.ArchiveIterator(f):
                    if record.rec_type == 'conversion':
                        content = record.content_stream().read().decode('utf-8')
                        self.con.execute("INSERT INTO wet_data VALUES (?, ?)", (record.rec_headers.get_header('WARC-Date'), content, ))
        except Exception as e:
            logging.error(f'Error processing wet file: {e}')

    def extract(self) -> List[Record]:
        result =  self.con.execute(self.sql, self.keywords).fetchall()
        return [Record(*x) for x in result]