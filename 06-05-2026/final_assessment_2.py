import dlt
from pyspark.sql.functions import *
@dlt.table
def bronze_orderss():
    data=[(301,"Hyderabad","GROCERIES",24000),(302,"Bengaluru","GROCERIES",31500),(303,"Delhi","ELECTRONICS",90000),(304,"Hyderabad","ELECTRONICS",125000),(305,"Pune","ELECTRONICS",186000)]
    columns=["order_id","city","category","bill_amount"]
    return spark.createDataFrame(data,columns)
@dlt.table
def silver_orderss():
    return dlt.read("bronze_orderss").filter(col("bill_amount").isNotNull())
@dlt.table
def silver_orders_revenues():
    return dlt.read("silver_orderss").withColumn("total_revenue",col("bill_amount"))
@dlt.table
def silver_cleaned_orderss():
    return dlt.read("silver_orders_revenues").filter(col("bill_amount")>0)
@dlt.table
def gold_city_revenues():
    return dlt.read("silver_cleaned_orderss").groupBy("city").agg(sum("total_revenue").alias("city_revenue"))
@dlt.table
def gold_category_revenue():
    return dlt.read("silver_cleaned_orderss").groupBy("category").agg(sum("total_revenue").alias("category_revenue"))









