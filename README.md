# Data Migration and Change Data Capture 
## **What this project does**
This project focuses on migrating data from MySQL to Databricks for analytics purposes while ensuring data synchronization and maintaining a comprehensive history of data changes. By leveraging the capabilities of Databricks and Delta Lake, this project provides an efficient and scalable solution for tracking and processing data changes. 

## **How this project works**
### Data Migration 
Databricks was connected to the MySQL database using MySQL JDBC driver. Data in MySQL database were then read to Databricks using PySpark direct queries. 
### Change Data Capture 
Tow Slowly Changing Dimension (SCD) tables were implemented to effectively track and manage the changes in data.

First, a SCD Type 1 table was developed using the SQL Merge function. The merge function allows handling insertions, updates, and deletions in a single operation. The SCD 1 table tracks changes in data over time while overwriting the existing data with new values. This mechanism ensures that the most recent information is stored and available for analysis.
