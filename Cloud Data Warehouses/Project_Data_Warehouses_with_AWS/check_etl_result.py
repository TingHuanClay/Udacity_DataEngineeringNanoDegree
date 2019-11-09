import configparser
import psycopg2
from sql_queries import data_count_queries, data_count_queries_titles


def check_results(cur, conn):
    """
    Description:
        Check result via querying each table data counts
    """
    index = 0
    for query in data_count_queries:
        print("{}. query {}: {}".format(index + 1, data_count_queries_titles[index], query))
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print("   Data Counts: ", row)

        index = index + 1


def main():
    """
    Validate the result of the ETL by running query the data count of each table (staging, dimension and fact table.)
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    check_results(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()