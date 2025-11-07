import os, pyspark

print("SPARK_HOME =", os.environ.get("SPARK_HOME"))
print("PYTHONPATH =", os.environ.get("PYTHONPATH"))
print("PySpark path =", pyspark.__file__)