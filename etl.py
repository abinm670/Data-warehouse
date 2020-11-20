import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

''' Loop inside the copy_table_queries list and execute each index
copy from s3 to staging tables '''


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as err:
            print("cant load", err)
            conn.rollback()


''' Loop inside the insert_table_queries list and execute each index
insert data from staging tables to star schema '''


def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as errorMsg:
            print(errorMsg, "instert issue")
            conn.rollback()


'''The main function will log to the star schema on Redshift(AWS) \
    two functions:
          * 1- load the data from stanging tables 
          * 2- insert data into the star Schema  
     including two arguments in each method:
        * 1- connection to the database using the VPC
        * 2- cursor to execute the SQL query '''


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} \
        password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
