#!/usr/bin/env python
# coding: utf-8

# In[28]:


from pyspark.sql import SparkSession


# In[29]:


spark = SparkSession.builder.appName('basics').getOrCreate()


# In[30]:


df = spark.read.json('/home/tushar/Desktop/json/json-file-plus/package.json')


# In[31]:


df.printSchema()


# In[32]:


df.columns


# In[33]:


from pyspark.sql.types import StructField,StringType,IntegerType,StructType


# In[34]:


data = [StructField('_corrupt_record',IntegerType(),True)]


# In[35]:


finalSchemma = StructType(fields=data)


# In[36]:


df = spark.read.json('/home/tushar/Desktop/json/json-file-plus/package.json',schema=finalSchemma)


# In[37]:


df.printSchema()


# In[38]:


df.show()


# In[ ]:


df

