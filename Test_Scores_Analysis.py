# Databricks notebook source
#Prompts for File Path and File Name
dbutils.widgets.text("FilePath","/dbfs/mnt/sink_lake/raw-src/test/csv/")
dbutils.widgets.text("FileName","TestData.csv")

#Get prompt values and store them in respective variables
file_path = dbutils.widgets.get("FilePath")
file_name = dbutils.widgets.get("FileName")

# COMMAND ----------

#Combine file path and file name to be used in file operation
data_path = file_path + file_name #'/dbfs/mnt/sink_lake/raw-src/test/csv/TestData.csv'

#Open the file for reading data
data_file = open(data_path, "r")

#Read the contents of file into a string
data_lines_str = data_file.read()

#Display object type for data_lines_str
type(data_lines_str)

# COMMAND ----------

#Break down string object into list
data_lines_list = data_lines_str.split("\n")

#Create a dummy list object to be appended in the next steps
data_list=[]

#Iterate through the list of items and parse the values into fields
for data_line in data_lines_list:
    if (data_line != "First Name,Second Name,Score" and data_line != ""): #Exclude header row and Carriage return at the end of file if exists
        data_line_cols_split_list = data_line.split(",") #Split the list item to derive fields
        data_list.append(data_line_cols_split_list) #Append the parsed list of items

# COMMAND ----------

#Create a dataframe from the list created from the above steps
test_scores_df = spark.createDataFrame(data_list, ["FirstName", "SecondName", "Score"])

# COMMAND ----------

#Display contents of the test_scores_df dataframe
display(test_scores_df)

# COMMAND ----------

#Create view for the dataframe created at previous step
test_scores_df.createOrReplaceTempView("vw_test_scores_df")

# COMMAND ----------

#Derive the highest score and capture it into a variable
max_score = sqlContext.sql(f""" select max(score) from vw_test_scores_df """).distinct().collect()[0][0]

# COMMAND ----------

#Import required packages from pyspark
from pyspark.sql.functions import *

#Filter the records of top scorers and derive their FullName by concatenating FirstName and SecondName with a space in between and sort the result on FullName
top_scorers = test_scores_df.filter("score = " + max_score) \
                     .withColumn("FullName",concat(col("FirstName"),lit(' '),col("SecondName"))) \
                     .drop("FirstName") \
                     .drop("SecondName") \
                     .drop("score") \
                     .orderBy("FullName")

# COMMAND ----------

#Display the list of top scorers with results sorted on FullName
display(top_scorers)

# COMMAND ----------

#Create a dataframe which holds the highest score to be displayed in the required format
max_score_df = sqlContext.sql(f""" select concat('Score : ', max(score)) as max_score_string from vw_test_scores_df """).distinct()

# COMMAND ----------

#Display the output of above dataframe
display(max_score_df)

# COMMAND ----------

#Combine the two dataframes to hold the final output
final_output_df = top_scorers.union(max_score_df).withColumnRenamed("FullName","FinalOutput")

# COMMAND ----------

#Display the final output
display(final_output_df)
