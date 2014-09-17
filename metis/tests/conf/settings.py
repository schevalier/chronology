import os


DEBUG = True
PORT = 9192

EXECUTORS = ['python.PythonExecutor', 'spark.SparkExecutor']
DEFAULT_EXECUTOR = 'spark.SparkExecutor'

# Location of Spark home, where we can find PySpark.
SPARK_HOME = os.environ.get('SPARK_HOME', '/scratch/spark-1.0.0')
# Host name of the master node of your Spark cluster.
SPARK_MASTER = 'local'
