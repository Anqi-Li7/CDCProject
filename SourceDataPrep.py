# Databricks notebook source
from pyspark.sql.types import * 

# COMMAND ----------

#about this file: create source data: employee table in MySQL

# COMMAND ----------

jdbc_url = 'jdbc:mysql://database.ascendingdc.com:3306/retail_db'
user = 'student'
password = '1234abcd'
#jdbc_driver = "org.mariadb.jdbc.Driver"
jdbc_driver = "com.mysql.jdbc.Driver"

#define MySQL connection
mysql_properties = {
    'jdbc_url': 'jdbc:mysql://database.ascendingdc.com:3306/de_002',
    'jdbc_driver': "com.mysql.jdbc.Driver",
    'user': 'peekaboo15',
    'password': 'welcome',
    'dbtable': 'employee'
}

# COMMAND ----------

#define schema 
schema = StructType([
                    StructField('emp_id',IntegerType()),
                    StructField('fname',StringType()),
                    StructField('lname',StringType()),   
                    StructField('salary',IntegerType()),
                    StructField('dept_id',IntegerType())
          ])

# COMMAND ----------

#insert 10 rows into the table 
data = [(1, 'John', 'Doe', 50000, 101),
        (2, 'Jane', 'Smith', 60000, 102),
        (3, 'Bob', 'Johnson', 55000, 101),
        (4, 'Mary', 'Jones', 70000, 103),
        (5, 'Mike', 'Davis', 65000, 102),
        (6, 'Emily', 'Wilson', 45000, 101),
        (7, 'David', 'Brown', 80000, 103),
        (8, 'Sarah', 'Miller', 75000, 102),
        (9, 'Kevin', 'Lee', 60000, 101),
        (10, 'Lisa', 'Taylor', 85000, 103)
        ]

# COMMAND ----------

#create dataframe using data
df = spark.createDataFrame(data, schema = schema)

#write df to MySQL to create a table in MySQL
df.write.jdbc(url=mysql_properties['jdbc_url'],
            table = mysql_properties['dbtable'],
            mode = 'overwrite',
            properties = mysql_properties
            )
