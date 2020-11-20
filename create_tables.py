import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

''' Loop inside the drop_table_queries list and execute each index
drop tables '''


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


''' Loop inside the create_table_queries list and execute each index 
create tables'''


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


'''The main function will log to the star schema on Redshift(AWS) \
    two functions will be called within two arguments:
        * 1- connection to the database using the VPC
        * 2- cursor to execute the SQL query '''


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} \
        password={} port={}".format(*config['CLUSTER'].values()))

    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
