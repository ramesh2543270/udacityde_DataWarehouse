import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from Manage_RS_Cluster import get_Cluster_Props

#Config
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        #print("Running Query - {}".format(query))
        cur.execute(query)
        conn.commit()


def main():  
    
    DWH_ENDPOINT, _ = get_Cluster_Props()
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
                            DWH_ENDPOINT,
                            DWH_DB,
                            DWH_DB_USER,
                            DWH_DB_PASSWORD,
                            DWH_PORT
                            ))
    print("Got connection")
    cur = conn.cursor()
    print("Load staging tables")
    load_staging_tables(cur, conn)
    print("Completed load of staging tables")
    print("Inserting into main tables")
    insert_tables(cur, conn)
    print("Insertion into main tables completed")

    conn.close()


if __name__ == "__main__":
    main()
