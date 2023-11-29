import connection.connect as con
import datetime

class ETLProcess:
    def __init__(self):
        self.database_name = 'BHATBHATENI_DB'
        self.stage_name = 'MY_STAGE'
        self.stg_table = 'STG_REGION'
        self.temp_table = 'TMP_REGION'
        self.target_table = 'D_REGION_LU'
        self.staging_schema = 'STG'
        self.temp_schema = 'TMP'
        self.target_schema = 'TGT'
        self.csv_file = r'csv\REGION.csv'
        self.cur = con.connection()

    def truncate_table(self, table_name):
        print('Inside region_etl.py File.')
        print('Truncating the Table.')
        if (table_name[:3])=='STG': 
            query = f"TRUNCATE TABLE {self.database_name}.{self.staging_schema}.{self.stg_table}"
        elif (table_name[:3])=='TMP':
            query = f"TRUNCATE TABLE {self.database_name}.{self.temp_schema}.{self.temp_table}"
        else:
            query = f"TRUNCATE TABLE {self.database_name}.{self.target_schema}.{self.target_table}"
        self.cur.execute(query)

    def create_stage(self):
        print('Creating the Stage.')
        query = f"CREATE OR REPLACE STAGE {self.staging_schema}.{self.stage_name}"
        self.cur.execute(query)

    def load_data_to_staging(self):
        print('Loading data to the staging area.')
        put_query = f"PUT file://{self.csv_file} @{self.staging_schema}.{self.stage_name}"
        copy_query = f"""
            COPY INTO {self.staging_schema}.{self.stg_table}
            FROM @{self.staging_schema}.{self.stage_name}
            FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
        """
        self.cur.execute(put_query)
        self.cur.execute(copy_query)

    def insert_data_to_temp(self):
        print('Inserting data from Stage to Temporary Table.')
        insert_query = f"""
            INSERT INTO {self.temp_schema}.{self.temp_table}
            SELECT * FROM {self.staging_schema}.{self.stg_table}
        """
        self.cur.execute(insert_query)

    def upsert_data_to_target(self):
        print('Inserting or Updating the Target Table.')
        upsert_query = f"""
            MERGE INTO {self.target_schema}.{self.target_table} AS tgt
            USING {self.temp_schema}.{self.temp_table} AS tmp
            ON tgt.id = tmp.id
            WHEN MATCHED THEN UPDATE SET
                tgt.region_desc = tmp.region_desc,
                tgt.start_date = CURRENT_TIMESTAMP(),
                tgt.end_date = CURRENT_TIMESTAMP(),
                tgt.country_id = tmp.country_id,
                tgt.UPDATE_TS = CASE WHEN tgt.REGION_desc != tmp.REGION_desc THEN CURRENT_TIMESTAMP() ELSE tgt.UPDATE_TS END,
                tgt.ACTIVE_RECORD = TRUE
            WHEN NOT MATCHED THEN 
                INSERT (id, country_id, region_desc, INSERT_TS, UPDATE_TS, ACTIVE_RECORD) 
                VALUES (tmp.id, tmp.country_id, tmp.region_desc, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);
        """
        self.cur.execute(upsert_query)

    def update_target_data(self):
        print('Updating the target table when id not in the temp_table. i.e. Deleted Datas.')
        update_query = f"""
            UPDATE {self.target_schema}.{self.target_table}
            SET UPDATE_TS = CURRENT_TIMESTAMP(), END_DATE = CURRENT_TIMESTAMP(), ACTIVE_RECORD = FALSE
            WHERE id NOT IN (SELECT id FROM {self.temp_schema}.{self.temp_table});
        """
        self.cur.execute(update_query)

    def commit_and_close(self):
        print('Committing the Changes.')
        self.cur.connection.commit()
        self.cur.close()
