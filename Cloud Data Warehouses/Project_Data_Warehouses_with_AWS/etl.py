import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, copy_table_titles, insert_table_titles


def load_staging_tables(cur, conn):
    """
    Description:
        Loading data from s3 to staging tables in Redshift cluster
    """

    print('[ETL] 1. Loading Staging tables')
    index = 0
    for query in copy_table_queries:
        print("{}. {}".format(index + 1, copy_table_titles[index]))
        cur.execute(query)
        conn.commit()
        index = index + 1
    print('Finished: 1. Loading Staging tables')


def insert_tables(cur, conn):
    """
    Description:
        Select data from staging tables (staging_events & staging_songs)
    """
    print('[ETL] 2. Inserting DWH tables')
    index = 0
    for query in insert_table_queries:
        print("{}. {}".format(index + 1, insert_table_titles[index]))
        cur.execute(query)
        conn.commit()
        index = index + 1
    print('Finished: 2. Inserting DWH tables')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()