import dlt
from pyspark.sql.functions import *
@dlt.table
def bronze_patient_visitss():
    data=[
    (1,"Hyderabad","Cardiology",5200),
    (2,"Bengaluru","Dermatology",2800),
    (3,"Mumbai","Orthopedics",7500),
    (4,"Delhi","Pediatrics",2900),
    (5,"Chennai","Cardiology",5300)
    ]
    columns=["visit_id","city","specialization","bill_amount"]
    return spark.createDataFrame(data,columns)
@dlt.table
def silver_patient_visitss():
    return dlt.read("bronze_patient_visitss").filter(col("bill_amount").isNotNull())
@dlt.table
def silver_patient_visitss_total():
    return dlt.read("silver_patient_visitss").withColumn("total_bill",col("bill_amount")+500)
@dlt.table
def silver_cleaned_visitss():
    return dlt.read("silver_patient_visitss_total").filter(col("bill_amount")>0)
@dlt.table
def gold_city_revenue():
    return dlt.read("silver_cleaned_visitss").groupBy("city").agg(sum("total_bill").alias("total_revenue"))
@dlt.table
def gold_specialization_revenue():
    return dlt.read("silver_cleaned_visitss").groupBy("specialization").agg(sum("total_bill").alias("total_revenue"))










