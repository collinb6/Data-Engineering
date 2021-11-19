import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries



# bc - load_staging_tables - gets data from s3 and loads them into the redshift STAGING tables
# bc - sql_queries STAGING TABLES
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print("copying from s3 into staging table")
        cur.execute(query)
        conn.commit()

# bc - insert_tables - takes the date from the redshift STAGING tables, and transforms them into the STAR schema tables
# bc - in sql_queries FINAL TABLES
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print("inserting data into star schema table")
        cur.execute(query)
        conn.commit()


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