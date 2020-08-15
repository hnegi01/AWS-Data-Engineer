import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, StringType, IntegerType
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import DateType





## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "raw_oracle_test_inventory_activity", table_name = "document_hdr", transformation_ctx = "datasource0")
## Convert Dynamic Frame to Spark Data Frame
datasource1 = datasource0.toDF()

# ## Add additional columns
datasource1 = datasource1.withColumn('inserted_date', F.lit(F.current_timestamp()).cast(TimestampType()))
datasource1 = datasource1.withColumn('updated_date', F.lit(F.current_timestamp()).cast(TimestampType()))
datasource1 = datasource1.withColumn('source',F.lit('Oracle-LVI3PLT'))
datasource1 = datasource1.withColumn('migration_status',F.lit('FR'))
datasource1 = datasource1.withColumn('temp_stop_seq', datasource1['temp_stop_seq'].cast(IntegerType()))
datasource1 = datasource1.withColumn('transaction_count', datasource1['transaction_count'].cast(IntegerType()))

#datasource1.select (['inserted_date','updated_date','source']).show(5)

datasource1_notnull = datasource1.where(col("i3pl_chng_time").isNotNull())

datasource1_notnull = datasource1_notnull.withColumn('i3pl_chng_time', datasource1_notnull['i3pl_chng_time'].cast(StringType()))

datasource1_notnull = datasource1_notnull.withColumn("i3pl_chng_time", to_timestamp("i3pl_chng_time", "yyyyMMddHHmmss.0000000000"))

datasource1_notnull = datasource1_notnull.withColumn('i3pl_chng_time', datasource1_notnull['i3pl_chng_time'].cast(TimestampType()))

# #datasource1_notnull.select(['i3pl_chng_time']).show(10)


datasource1_null = datasource1.where(col("i3pl_chng_time").isNull())

datasource1_null = datasource1_null.withColumn('i3pl_chng_time', F.col('i3pl_chng_time').cast(TimestampType()))


# #datasource1_null.select(['i3pl_chng_time']).show(10)

concatenated_data = datasource1_null.union(datasource1_notnull)

#concatenated_data.select(['i3pl_chng_time','source']).show(5)



concatenated_data = concatenated_data .withColumn('DAY_OF_SHIP_RCV', F.lit(F.date_trunc("day",concatenated_data['ACTUAL_RCPT_SHIP_DATE'])).cast(TimestampType()))



#concatenated_data = concatenated_data.withColumn('row_status',F.lit('inserted'))

#concatenated_data.select(['row_status','i3pl_chng_time']).show(5)

#concatenated_data.dtypes

datasource2 = DynamicFrame.fromDF(concatenated_data, glueContext, "datasource2")

applymapping1 = ApplyMapping.apply(frame = datasource2, mappings = [("document_type", "string", "document_type", "string"), ("document_subtype", "string", "document_subtype", "string"), ("source_system_code", "string", "source_system_code", "string"), ("facility_id", "string", "facility_id", "string"), ("customer_id", "string", "customer_id", "string"), ("whse_ref_num", "string", "whse_ref_num", "string"), ("cust_ref_num", "string", "cust_ref_num", "string"), ("internal_id", "string", "internal_id", "string"), ("temperature_uom", "string", "temperature_uom", "string"), ("volume_uom", "string", "volume_uom", "string"), ("weight_uom", "string", "weight_uom", "string"), ("dimension_uom", "string", "dimension_uom", "string"), ("date_timezone", "string", "date_timezone", "string"), ("current_units", "decimal(13,0)", "current_units", "decimal(13,0)"), ("current_net_weight", "decimal(12,3)", "current_net_weight", "decimal(12,3)"), ("current_gross_weight", "decimal(12,3)", "current_gross_weight", "decimal(12,3)"), ("current_catch_weight", "decimal(12,3)", "current_catch_weight", "decimal(12,3)"), ("current_volume", "decimal(12,3)", "current_volume", "decimal(12,3)"), ("current_pallets", "decimal(13,0)", "current_pallets", "decimal(13,0)"), ("original_units", "decimal(13,0)", "original_units", "decimal(13,0)"), ("original_net_weight", "decimal(12,3)", "original_net_weight", "decimal(12,3)"), ("original_gross_weight", "decimal(12,3)", "original_gross_weight", "decimal(12,3)"), ("original_catch_weight", "decimal(12,3)", "original_catch_weight", "decimal(12,3)"), ("original_volume", "decimal(12,3)", "original_volume", "decimal(12,3)"), ("original_pallets", "decimal(13,0)", "original_pallets", "decimal(13,0)"), ("revised_units", "decimal(13,0)", "revised_units", "decimal(13,0)"), ("revised_net_weight", "decimal(12,3)", "revised_net_weight", "decimal(12,3)"), ("revised_gross_weight", "decimal(12,3)", "revised_gross_weight", "decimal(12,3)"), ("revised_catch_weight", "decimal(12,3)", "revised_catch_weight", "decimal(12,3)"), ("revised_volume", "decimal(12,3)", "revised_volume", "decimal(12,3)"), ("revised_pallets", "decimal(13,0)", "revised_pallets", "decimal(13,0)"), ("stop_seq", "decimal(15,0)", "stop_seq", "decimal(15,0)"), ("temp_stop_seq", "int", "temp_stop_seq", "int"), ("load_seq", "decimal(15,0)", "load_seq", "decimal(15,0)"), ("seal_1", "string", "seal_1", "string"), ("seal_status_1", "string", "seal_status_1", "string"), ("seal_2", "string", "seal_2", "string"), ("seal_status_2", "string", "seal_status_2", "string"), ("bol", "string", "bol", "string"), ("master_bol", "string", "master_bol", "string"), ("shipment_id", "string", "shipment_id", "string"), ("wms_load_num", "string", "wms_load_num", "string"), ("tracking_num", "string", "tracking_num", "string"), ("transp_load_num", "string", "transp_load_num", "string"), ("usda_certification_num", "string", "usda_certification_num", "string"), ("master_link", "string", "master_link", "string"), ("vendor_consig_refnum", "string", "vendor_consig_refnum", "string"), ("vendor_consig_id", "string", "vendor_consig_id", "string"), ("carrier_refnum", "string", "carrier_refnum", "string"), ("carrier_id", "string", "carrier_id", "string"), ("broker_id", "string", "broker_id", "string"), ("appt_id", "string", "appt_id", "string"), ("internal_appt_id", "string", "internal_appt_id", "string"), ("transp_mode", "string", "transp_mode", "string"), ("prev_status", "string", "prev_status", "string"), ("status", "string", "status", "string"), ("status_date", "timestamp", "status_date", "timestamp"), ("not_before_date", "timestamp", "not_before_date", "timestamp"), ("not_later_date", "timestamp", "not_later_date", "timestamp"), ("appt_date", "timestamp", "appt_date", "timestamp"), ("expected_rcpt_ship_date", "timestamp", "expected_rcpt_ship_date", "timestamp"), ("actual_rcpt_ship_date", "timestamp", "actual_rcpt_ship_date", "timestamp"), ("started_picking", "timestamp", "started_picking", "timestamp"), ("started_load_unload", "timestamp", "started_load_unload", "timestamp"), ("completed_load_unload", "timestamp", "completed_load_unload", "timestamp"), ("completed_receiving", "timestamp", "completed_receiving", "timestamp"), ("temp_nose_min", "decimal(9,3)", "temp_nose_min", "decimal(9,3)"), ("temp_nose_max", "decimal(9,3)", "temp_nose_max", "decimal(9,3)"), ("temp_mid_min", "decimal(9,3)", "temp_mid_min", "decimal(9,3)"), ("temp_mid_max", "decimal(9,3)", "temp_mid_max", "decimal(9,3)"), ("temp_tail_min", "decimal(9,3)", "temp_tail_min", "decimal(9,3)"), ("temp_tail_max", "decimal(9,3)", "temp_tail_max", "decimal(9,3)"), ("misc_refnum1_type", "string", "misc_refnum1_type", "string"), ("misc_refnum1", "string", "misc_refnum1", "string"), ("misc_refnum2_type", "string", "misc_refnum2_type", "string"), ("misc_refnum2", "string", "misc_refnum2", "string"), ("misc_refnum3_type", "string", "misc_refnum3_type", "string"), ("misc_refnum3", "string", "misc_refnum3", "string"), ("misc_refnum4_type", "string", "misc_refnum4_type", "string"), ("misc_refnum4", "string", "misc_refnum4", "string"), ("misc_refnum5_type", "string", "misc_refnum5_type", "string"), ("misc_refnum5", "string", "misc_refnum5", "string"), ("ats_load_flag", "string", "ats_load_flag", "string"), ("creator_id", "string", "creator_id", "string"), ("last_updater_id", "string", "last_updater_id", "string"), ("create_date", "timestamp", "create_date", "timestamp"), ("last_update_date", "timestamp", "last_update_date", "timestamp"), ("vendor_shipment_date", "timestamp", "vendor_shipment_date", "timestamp"), ("rp_shipmnt_early_shipdte", "timestamp", "rp_shipmnt_early_shipdte", "timestamp"), ("rp_shipmnt_late_shipdte", "timestamp", "rp_shipmnt_late_shipdte", "timestamp"), ("oe_tran_id", "string", "oe_tran_id", "string"), ("payment_terms", "string", "payment_terms", "string"), ("creator_type", "string", "creator_type", "string"), ("document_subtype_category", "string", "document_subtype_category", "string"), ("transaction_count", "int", "transaction_count", "int"), ("DAY_OF_SHIP_RCV", "timestamp", "DAY_OF_SHIP_RCV", "timestamp"),("document_subtype_desc", "string", "document_subtype_desc", "string"), ("i3pl_chng_time", "timestamp", "i3pl_chng_time", "timestamp"), ("inserted_date", "timestamp", "inserted_date", "timestamp"), ("updated_date", "timestamp", "updated_date", "timestamp"), ("source", "string", "source", "string"),("migration_status","string","migration_status","string")], transformation_ctx = "applymapping1")

#datasource_test = applymapping1.toDF()


#datasource_test.show(5)


#non_partioned_data = applymapping1.repartition(1)

current_date = datetime.now()

datasink4 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": f"s3://i3pl-migration/Curated/document_hdr/full_load/{current_date}"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()
