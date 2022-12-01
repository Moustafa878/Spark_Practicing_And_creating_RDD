# Databricks notebook source
nums = sc.parallelize([1, 2, 3])

# COMMAND ----------

squares = nums.map(lambda x: x*x)
even = squares.filter(lambda x: x % 2 == 0) 
megred = nums.reduce(lambda x, y: x + y) 

# COMMAND ----------

print(squares.collect())


# COMMAND ----------

print(even.collect())


# COMMAND ----------

nums.take(2) 

# COMMAND ----------

nums.count()

# COMMAND ----------

print(megred)
nums.reduce(lambda x, y: x + y)

# COMMAND ----------


