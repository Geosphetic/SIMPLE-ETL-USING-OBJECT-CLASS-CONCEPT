import snowflake.connector

def connection():
    con = snowflake.connector.connect(
    user = 'YourSnowflakeID',
    password = 'YourSnowflake Password',
    account = 'Your Snowflake Account', #in URL
    database = 'BHATBHATENI_DB',
    source_schema = 'BHATBHATENI_DB.TRANSACTIONS',
    staging_schema = 'BHATBHATENI_DB.STG',
    temp_schema = 'BHATBHATENI_DB.TMP',
    target_schema = 'BHATBHATENI_DB.TGT'
    )
    cur=con.cursor()
    return cur

