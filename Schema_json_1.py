#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession


# In[3]:


spark = SparkSession.builder.appName('basics').getOrCreate()


# In[5]:


df = spark.read.json('/home/tushar/Desktop/spark-and-python-for-big-data-with-pyspark/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/people.json')


# In[7]:


df.show() 


# In[29]:


df.printSchema()


# In[30]:


df.columns


# In[31]:


df.describe()


# In[32]:


df.describe().show()


# In[34]:


#From this we are changing the schemma for the file
#structfield is used to change the structure of the fields
#Structtype is used to
from pyspark.sql.types import StructField,StringType,IntegerType,StructType


# 
# 

# In[35]:


data_schemma = [StructField('age',IntegerType(),True),StructField('name',StringType(),True)]


# In[38]:


#giving the object for the sche
finalSchemma = StructType(fields=data_schemma)


# In[39]:


df =spark.read.json('/home/tushar/spark-2.3.0-bin-hadoop2.7/python/Python-and-Spark-for-Big-Data-master/Spark_DataFrames',schema=finalSchemma)


# In[40]:


df.printSchema()


# In[ ]:




