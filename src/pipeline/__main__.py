from src import env  # env 호출
from src.utils import connector
from src.pipeline._01_dl_data_lake import backfill, incremental

def main():
    # connectors
    conn_gspread = connector.get_client_gspread()

    # Data Lake
    test = backfill.run(client=conn_gspread)

    ## incremental
    ## backfill (insert)
    ## backfill (delete)

    # Data Preprocessing

    # Data Warehouse

    # Data Mart
    print(test)

    ...

if __name__ == "__main__":
    main()