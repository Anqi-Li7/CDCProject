# Databricks notebook source
import mysql.connector 

# COMMAND ----------

# MAGIC %run "./2-SCD1, SCD2 Functions"

# COMMAND ----------

#read source data from MySQL
mysql_properties = {
    'jdbc_url': 'jdbc:mysql://database.ascendingdc.com:3306/de_002',
    'jdbc_driver': "com.mysql.jdbc.Driver",
    'user': 'peekaboo15',
    'password': 'welcome',
    'dbtable': 'employee'
}
employee_df = (spark.read.jdbc(
                            url=mysql_properties['jdbc_url'],
                            table = mysql_properties['dbtable'],
                            properties = mysql_properties)
                )
#check schema and preview of dataframe
employee_df.printSchema()
display(employee_df)

# COMMAND ----------

#craete dataframe as a sql table in delta lake 
employee_df.createOrReplaceTempView('employee_mysql')

# COMMAND ----------

# MAGIC %sql
# MAGIC --create database in delta lake
# MAGIC CREATE DATABASE IF NOT EXISTS de_002;

# COMMAND ----------

# MAGIC %sql
# MAGIC --create employee table in database, this table will be a SCD1 table  
# MAGIC CREATE TABLE IF NOT EXISTS de_002.employee (
# MAGIC   emp_id INT,
# MAGIC   fname STRING,
# MAGIC   lname STRING,
# MAGIC   salary INT,
# MAGIC   dept_id INT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC --enable change data feed to capture data change over time
# MAGIC ALTER TABLE de_002.employee SET TBLPROPERTIES(delta.enableChangeDataFeed = true)

# COMMAND ----------

#merge from source table into scd1
target_table = "de_002.employee"
source_table = "employee_mysql"
primary_keys = ["emp_id"]
columns = ['fname', 'lname', 'salary', 'dept_id']

merge_to_scd1(target_table, source_table, primary_keys, columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC --check table change history 
# MAGIC DESCRIBE HISTORY de_002.employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC --check SCD1 table after merge 
# MAGIC select * from de_002.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC --check table change history 
# MAGIC DESCRIBE HISTORY de_002.employee;

# COMMAND ----------

#get CDF after merge to SCD1 table
getCDF("SCD1_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SCD1_view

# COMMAND ----------

# MAGIC %sql
# MAGIC --create SCD 2 table 
# MAGIC CREATE TABLE IF NOT EXISTS de_002.employee_scd2 (
# MAGIC   emp_id INT,
# MAGIC   fname STRING,
# MAGIC   lname STRING,
# MAGIC   salary INT,
# MAGIC   dept_id INT,
# MAGIC   start_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP,
# MAGIC   is_current BOOLEAN
# MAGIC );

# COMMAND ----------

#merge from CDF of SCD1 table to SCD2 table 
target_table = "de_002.employee_scd2"
source_table = "SCD1_view"
primary_keys = ["emp_id"]
columns = ['fname', 'lname', 'salary', 'dept_id']

merge_to_scd2(target_table, source_table, primary_keys, columns)

# COMMAND ----------

#make some changes in the MySQL source data
mydb = mysql.connector.connect(
    host = 'database.ascendingdc.com',
    port = 3306,
    user = 'peekaboo15',
    password = 'welcome',
    database = 'de_002'
)

#create cursor object 
mycursor = mydb.cursor()

#update data
mycursor.execute("UPDATE de_002.employee SET salary = 65001 WHERE emp_id =1")
#insert row 
mycursor.execute("INSERT INTO employee (emp_id, fname, lname, salary, dept_id) VALUES (15, 'Jim', 'Wong', 4000, 101)")
#delete row
mycursor.execute("DELETE FROM employee WHERE emp_id = 2")

#commit changes to database
mydb.commit()
#check rows affected
print(mycursor.rowcount, 'record(s) affected')

# COMMAND ----------

#read updated source data from MySQL
updated_df = (spark.read.jdbc(
                            url=mysql_properties['jdbc_url'],
                            table = mysql_properties['dbtable'],
                            properties = mysql_properties)
                )
#check schema and preview of dataframe
updated_df.printSchema()
display(updated_df)
#create a sql view 
updated_df.createOrReplaceTempView('employee_updated')

# COMMAND ----------

#merge updated source data from MySQL to SCD1 table
target_table = "de_002.employee"
source_table = "employee_updated"
primary_keys = ["emp_id"]
columns = ['fname', 'lname', 'salary', 'dept_id']

merge_to_scd1(target_table, source_table, primary_keys, columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC --check if meger to SCD1 was successful 
# MAGIC describe history de_002.employee
# MAGIC --check SCD1 table after merge 
# MAGIC select * from de_002.employee

# COMMAND ----------

#get CDF from the latest version of SCD1 table
getCDF("SCD1_view_updated")

# COMMAND ----------

#merge from latest CDF of SCD1 table to SCD2 table 
target_table = "de_002.employee_scd2"
source_table = "SCD1_view_updated"
primary_keys = ["emp_id"]
columns = ['fname', 'lname', 'salary', 'dept_id']

merge_to_scd2(target_table, source_table, primary_keys, columns)


# COMMAND ----------

# MAGIC %sql
# MAGIC --check if meger to SCD2 was successful 
# MAGIC describe history de_002.employee_scd2
# MAGIC --check SCD2 table after merge 
# MAGIC select * from de_002.employee_scd2
