#basic data frames code
#the below code is to be executed in the pySpark shell

#make sure to setup pySpark for DataFrames
#sudo cp /etc/hive/conf.dist/hive-site.xml /usr/lib/spark/conf

#execute the following to check the working of Data Frames
#sqlCtx.createDataFrame([("Somekey", 1)])
#out[]: DataFrames[_1:string, _2:bigint]

#creating tabular data
students = sc.parallelize([
[100, "Alice", 8.5, "Computer Science"], 
[101, "Bob", 7.1, "Engineering"], 
[102, "Carl", 6.2, "Engineering"]
])


#create a basic data frame with column names
students_df = sqlCtx.createDataFrame(students, ["id", "name", "grade", "degree"])
#in the above case the column type is automatically inferred by spark

#print the inferred schema information
students_df.printSchema()

#For more options on creating Data Frames
#sqlCtx.createDataFrame?

#Mean of a column using Spark Data Frames - mean of the "grade" column
students_df.agg({"grade":"mean"}).collect()

#To get a list of all available operations on dataframes
#from pyspark.sql import functions as F
#dir(F)

#Maximum grade by each degree
students_df.groupBy("degree").max("grade").collect()

#Pretty print - tabulated output
students_df.groupBy("degree").max("grade").show()

