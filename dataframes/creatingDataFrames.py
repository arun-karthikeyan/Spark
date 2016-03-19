#the below code is to be executed in the pySpark shell

#creating tabular data
students = sc.parallelize([
[100, "Alice", 8.5, "Computer Science"], 
[101, "Bob", 7.1, "Engineering"], 
[102, "Carl", 6.2, "Engineering"]
])

#specifying a schema for dataframes
from pyspark.sql.type import *
#the StructType class is used to specify the schema information
#the StructField class is used to mainly specify the name and datatype of each column
#StructField(String name, DataType dataType, boolean nullable, Metadata metadata)
schema = StructType([
StructField("id", LongType(), True), 
StructField("name", StringType(), True), 
StructField("grade", DoubleType(), True), 
StructField("degree", StringType(), True)
])

#creating a dataframe with the above schema
students_df = sqlCtx.createDataFrame(students, schema)
