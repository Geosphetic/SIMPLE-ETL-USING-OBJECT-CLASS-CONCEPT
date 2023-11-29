from etl.category_etl import ETLProcess as CategoryETL
from etl.country_etl import ETLProcess as CountryETL
from etl.customer_etl import ETLProcess as CustomerETL
from etl.product_etl import ETLProcess as ProductETL
from etl.region_etl import ETLProcess as RegionETL
from etl.store_etl import ETLProcess as StoreETL
from etl.subcategory_etl import ETLProcess as SubcategoryETL
from etl.fact_sales_etl import ETLProcess as FactSalesETL
from etl.fact_agg_sales_etl import ETLProcess as FactAggSalesETL

def main():
    etl_processes = [
        CategoryETL()
        ,CountryETL()
        ,CustomerETL()
        ,ProductETL()
        ,RegionETL()
        ,StoreETL()
        ,SubcategoryETL()
        ,FactSalesETL()
        ,FactAggSalesETL()
    ]

    for etl in etl_processes:
        etl.truncate_table('STG')
        etl.create_stage()
        etl.load_data_to_staging()
        etl.truncate_table('TMP')
        etl.insert_data_to_temp()
        etl.upsert_data_to_target()
        etl.update_target_data()
        etl.commit_and_close()

if __name__ == "__main__":
    main()
