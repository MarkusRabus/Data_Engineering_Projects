import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import os

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and creates the sparkify database on AWS. 
    
    - Establishes connection to database on AWS redshift cluster and get cursor.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """

	# Read configuration file and assign variables
    config = configparser.ConfigParser()
    config.read_file(open( os.path.expanduser('~/dwh.cfg') ))

    # connect to AWS database and return cursor
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    # close database connection
    conn.close()


if __name__ == "__main__":
    main()