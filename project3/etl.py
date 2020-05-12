import os
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copies json files into stagiging tables using the queries in `copy_table_queries` list. 
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert rows into facts and dimsension tables using the queries in `insert_table_queries` list. 
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Populates tables, created with `create_tables.py`, of the sparkify database on AWS. 
    
    - Establishes connection to database on AWS redshift cluster and get cursor.  
    
    - Copy json files in S3 bucket into staging tables.  
    
    - Uses staging tables to insert rows into facts and dimension tables. 
    
    - Finally, closes the connection. 
    """

    # Read configuration file and assign variables
    config = configparser.ConfigParser()
    config.read_file(open( os.path.expanduser('~/dwh.cfg') ))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # connect to AWS database and return cursor
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # close database connection
    conn.close()


if __name__ == "__main__":
    main()