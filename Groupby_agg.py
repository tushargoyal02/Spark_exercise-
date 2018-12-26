#!/usr/bin/env python
# coding: utf-8

# In[119]:


from pyspark.sql import SparkSession


# In[120]:


spark = SparkSession.builder.appName('group_by ').getOrCreate()


# In[121]:


df = spark.read.csv('/home/tushar/spark-2.3.0-bin-hadoop2.7/python/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/sales_info.csv',inferSchema=True,header=True)


# In[122]:


df.show()


# In[123]:


df.columns


# In[124]:


df.describe().show()


# In[125]:


df.printSchema()


# In[126]:


df.groupBy("Company").mean().show()


# In[127]:



df.groupBy("Company").avg().show()


# In[128]:


df.agg({'Sales':'sum'}).show()


# In[129]:


group_data = df.groupBy('Company')


# In[130]:


group_data.agg({'Sales':'max'}).show()


# In[131]:


from pyspark.sql.functions import (countDistinct,avg,stddev)


# In[132]:


df.select(countDistinct('Sales')).show()


# In[133]:


df.select(avg('Sales')).show()


# In[134]:


df.describe().show()


# In[135]:


df.select(stddev('Sales')).show()


# In[136]:


df.select(stddev('Sales').alias('Standard deviation')).show()


# In[137]:


from pyspark.sql.functions import format_number


# In[138]:


sales_std = df.select(stddev("Sales").alias('std'))


# In[139]:


sales_std.select(format_number('std',2).alias('Std')).show()


# In[140]:


df.show()


# In[141]:


df.orderBy(df['sales'].desc()).show()


# In[142]:


data = df.orderBy(df['Sales']).collect()


# In[118]:


data[1][2]


# In[ ]:




