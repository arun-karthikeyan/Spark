#Loading a json format into a dataframe

students_JSON = [
{"id":100, "name":"Alice", "grade":8.5, "degree":"Computer Science"}, 
{"id":101, "name":"Bob", "grade":7.1, "degree":"Engineering"}, 
{"id":102, "name":"Carl", "grade":6.2, "degree":"Engineering"}
]

students_json_df = sqlCtx.createDataFrame(students_JSON)

#Loading from an external JSON file - make sure the file exists in the path
students_json_df_2 = sqlCtx.jsonFile("file:///home/cloudera/Spark/dataframes/json-data/students.json")


#Loading a CSV File
# - Loader not included with the Spark Core
# - Using external package 'spark-csv', package information available at 'www.spark-packages.org'
# restart pySpark shell with the following parameters
#PYSPARK_DRIVER_PYTHON=ipython pyspark --packages com.databricks:spark-csv_2.10:1.4.0

#loading sample yelp csv
yelp_df = sqlCtx.load(source="com.databricks.spark.csv", header='true', inferSchema='true', path='file:///home/cloudera/Spark/dataframes/csv-data/yelp_data.csv')

#check out the automatically inferred schema
yelp_df.printSchema()

#check record count
yelp_df.count()
