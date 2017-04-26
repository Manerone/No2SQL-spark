import os
current_path = os.path.dirname(os.path.realpath(__file__))
import sys
sys.path.append(current_path + '/../../.')
from no2sql import No2SQL
from pyspark import SparkContext, SQLContext

sc = SparkContext()
sql_context = SQLContext(sc)
print 'Reading file', current_path + '/flights.csv.gz'
df = sql_context.read.format("com.databricks.spark.csv").options(
    header=True, inferSchema=True
    ).load(current_path + '/flights.csv.gz')
print 'Creating No2SQL object'
no2sql = No2SQL(df, sc)
print 'No2SQL.start()'
no2sql.start()
