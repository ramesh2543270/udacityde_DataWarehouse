import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from Manage_RS_Cluster import get_Cluster_Props

#Config
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    DWH_ENDPOINT, _ = get_Cluster_Props()
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
                            DWH_ENDPOINT,
                            DWH_DB,
                            DWH_DB_USER,
                            DWH_DB_PASSWORD,
                            DWH_PORT
                            ))
    cur = conn.cursor()
    print("Got connection")
    print("dropping tables")
    drop_tables(cur, conn)
    print("creating tables")
    create_tables(cur, conn)
    print("closing connection")
    conn.close()


if __name__ == "__main__":
    main()
