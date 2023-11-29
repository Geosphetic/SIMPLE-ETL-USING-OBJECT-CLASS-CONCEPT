import connection.connect as con
import datetime

class ETLProcess:
    def __init__(self):
        self.database_name = 'BHATBHATENI_DB'
        self.stage_name = 'MY_STAGE'
        self.stg_table = 'STG_SALES'
        self.temp_table = 'TMP_SALES'
        self.target_table = 'F_BHATBHATENI_AGG_SLS_PLC_MONTH_T'
        self.staging_schema = 'STG'
        self.temp_schema = 'TMP'
        self.target_schema = 'TGT'
        self.csv_file = r'csv\SALES.csv'
        self.cur = con.connection()

    def truncate_table(self, table_name):
        print('Inside fact_agg_sales_etl.py File.')
        print('Truncating the Table.')
        query = f"TRUNCATE TABLE {self.database_name}.{self.target_schema}.{self.target_table}"
        self.cur.execute(query)

    def create_stage(self):
        pass

    def load_data_to_staging(self):
        pass

    def insert_data_to_temp(self):
        pass

    def upsert_data_to_target(self):
        print('Inserting or Updating the Target Table.')
        upsert_query = f"""INSERT INTO {self.database_name}.{self.target_schema}.{self.target_table} 
                SELECT ID, STORE_ID, PRODUCT_ID, CUSTOMER_ID, MONTHNAME(TRANSACTION_TIME) Month_Name, SUM(QUANTITY), SUM(AMOUNT), SUM(DISCOUNT), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                FROM {self.database_name}.{self.temp_schema}.{self.temp_table}
                GROUP BY ID, STORE_ID, PRODUCT_ID, CUSTOMER_ID, Month_Name
                ORDER BY ID;
                """
        self.cur.execute(upsert_query)

    def update_target_data(self):
        pass

    def commit_and_close(self):
        print('Committing the Changes.')
        self.cur.connection.commit()
        self.cur.close()
