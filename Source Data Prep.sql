-- Databricks notebook source
-- MAGIC %python 
-- MAGIC from pyspark.sql.types import * 

-- COMMAND ----------

--about this file: create source data: employee table in MySQL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC jdbc_url = 'jdbc:mysql://database.ascendingdc.com:3306/retail_db'
-- MAGIC user = 'student'
-- MAGIC password = '1234abcd'
-- MAGIC #jdbc_driver = "org.mariadb.jdbc.Driver"
-- MAGIC jdbc_driver = "com.mysql.jdbc.Driver"
-- MAGIC
-- MAGIC #define MySQL connection
-- MAGIC mysql_properties = {
-- MAGIC     'jdbc_url': 'jdbc:mysql://database.ascendingdc.com:3306/de_002',
-- MAGIC     'jdbc_driver': "com.mysql.jdbc.Driver",
-- MAGIC     'user': 'peekaboo15',
-- MAGIC     'password': 'welcome',
-- MAGIC     'dbtable': 'employee'
-- MAGIC }

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC #define schema 
-- MAGIC schema = StructType([
-- MAGIC                     StructField('emp_id',IntegerType()),
-- MAGIC                     StructField('fname',StringType()),
-- MAGIC                     StructField('lname',StringType()),   
-- MAGIC                     StructField('salary',IntegerType()),
-- MAGIC                     StructField('dept_id',IntegerType())
-- MAGIC           ])

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC #insert 10 rows into the table 
-- MAGIC data = [(1, 'John', 'Doe', 50000, 101),
-- MAGIC         (2, 'Jane', 'Smith', 60000, 102),
-- MAGIC         (3, 'Bob', 'Johnson', 55000, 101),
-- MAGIC         (4, 'Mary', 'Jones', 70000, 103),
-- MAGIC         (5, 'Mike', 'Davis', 65000, 102),
-- MAGIC         (6, 'Emily', 'Wilson', 45000, 101),
-- MAGIC         (7, 'David', 'Brown', 80000, 103),
-- MAGIC         (8, 'Sarah', 'Miller', 75000, 102),
-- MAGIC         (9, 'Kevin', 'Lee', 60000, 101),
-- MAGIC         (10, 'Lisa', 'Taylor', 85000, 103)
-- MAGIC         ]

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC #create dataframe using data
-- MAGIC df = spark.createDataFrame(data, schema = schema)
-- MAGIC
-- MAGIC #write df to MySQL to create a table in MySQL
-- MAGIC df.write.jdbc(url=mysql_properties['jdbc_url'],
-- MAGIC             table = mysql_properties['dbtable'],
-- MAGIC             mode = 'overwrite',
-- MAGIC             properties = mysql_properties
-- MAGIC             )
