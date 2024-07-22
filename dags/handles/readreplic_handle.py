import pandas as pd
import mysql.connector
from loguru import logger
import os
from dotenv import load_dotenv

class ReadReplicHandle:
    def __init__(self, host, port, user, passwd, database, query, fields):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.database = database
        self.query = query
        self.fields = fields
        self.connection = None
        self.engine = None
    
    def create_connection(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            database=self.database
        )
        if self.connection:
            logger.info(f"Connected to MySql")
        else:
            logger.error("Something went wrong!")
    
    def query_data(self):
        cursor = self.connection.cursor()
        cursor.execute(self.query)
        result = pd.DataFrame(cursor.fetchall(), columns=self.fields)
        cursor.close()
        return result
    
if __name__ == "__main__":

    load_dotenv()

    host = os.getenv('READREPLIC_HOST')
    port = os.getenv('READREPLIC_PORT')
    user = os.getenv('READREPLIC_USER')
    passwd = os.getenv('READREPLIC_PASSWORD')
    database = os.getenv('READREPLIC_DB')
    
    fields=['id', 'name', 'product_type']

    data = ReadReplicHandle(host, port, user, passwd, database, query='SELECT id, name, product_type FROM products limit 10', fields=fields)
    data.create_connection()

    df = data.query_data()
    print(df)
    
 