# Databricks notebook source
import mysql.connector 

# COMMAND ----------

#create function to merge source data from MySQL to SCD1 table in delta lake 
def merge_to_scd1(target_table, source_table, primary_keys, columns):
    query_p0 = f""" 
    MERGE INTO {target_table} AS target
    USING {source_table} AS source
    """
    query_p1 = "ON "
    first_key = True
    for i in primary_keys:
        if first_key:
            query_p1 += f"source.{i} = target.{i}"
            first_key = False
        else:
            query_p1 += f" AND source.{i} = target.{i}"

    query_p2 = """
    WHEN MATCHED AND 
    """

    query_p3 = "("
    first_column = True
    for i in columns:
        if first_column:
            query_p3 += f"source.{i} <> target.{i}"
            first_column = False
        else:
            query_p3 += f" OR source.{i} <> target.{i}"

    query_p3 += ")"
    
    query_p4 = """
    THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    """
    query = query_p0 + query_p1 + query_p2 + query_p3 + query_p4
    try:
        spark.sql(query)
        return True
    except Exception as error:
        print(f"an error occurred, error = {error}")
        return False

# COMMAND ----------

#create function to get change data feed of SCD1 table and save as a sql table 
def getCDF(viewname):
    try:
        LastestVersion = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
        df = spark.sql(f"SELECT * FROM table_changes('de_002.employee', {LastestVersion}, {LastestVersion})")
        df.createOrReplaceTempView(viewname)
        return True
    except Exception as error:
        print(f"an error occurred, error = {error}")
        return False


# COMMAND ----------

#create function to merge from the CDF of SCD1 table to SDC2 table 
def merge_to_scd2(target_table, source_table, primary_keys, columns):
    query_p0 = f"""
    MERGE INTO {target_table} AS target
    USING (SELECT * FROM {source_table} WHERE _change_type != 'update_preimage' ) AS source 
    """

    query_p1 = "ON "
    first_key = True
    for i in primary_keys:
        if first_key:
            query_p1 += f"source.{i} = target.{i}"
            first_key = False
        else:
            query_p1 += f" AND source.{i} = target.{i}"

    #for rows deleted
    query_p2 = """
    WHEN MATCHED AND source._change_type = 'delete'
    THEN UPDATE SET
        target.end_date = CURRENT_TIMESTAMP,
        target.is_current = FALSE 
    """

    #for rows updated
    query_p3 = """
    WHEN MATCHED AND target.is_current = TRUE AND source._change_type = 'update_postimage'
    THEN UPDATE SET
        target.end_date = CURRENT_TIMESTAMP,
        target.is_current =  FALSE
    """

    #for new rows inserted
    query_p4 = """
    WHEN NOT MATCHED AND source._change_type = 'insert'
    THEN INSERT ( 
    """
    for i in primary_keys:
        query_p4 += f"{i}, "
    for j in columns:
        query_p4 += f"{j}, "
    query_p4 += "start_date, end_date, is_current) VALUES ( "
    for i in primary_keys:
        query_p4 += f"{i}, "
    for j in columns:
        query_p4 += f"source.{j}, "
    query_p4 += "CURRENT_TIMESTAMP, NULL, TRUE); "

    query_p5 = f"""
    INSERT INTO {target_table}
    SELECT
    """
    for i in primary_keys:
        query_p5 += f"{i}, "
    for j in columns:
        query_p5 += f"{j}, "
    query_p5 += f"""
    CURRENT_TIMESTAMP, NULL, True FROM {source_table}
    WHERE _change_type = 'update_postimage'
    """

    query = query_p0 + query_p1 + query_p2 + query_p3 + query_p4
    
    try:
        spark.sql(query)
        spark.sql(query_p5)
        return True
    except Exception as error:
        print(f"an error occurred, error = {error}")
        return False
