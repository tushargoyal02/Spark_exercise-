#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession.builder.appName('basics').getOrCreate()


# In[4]:


df = spark.read.csv('/home/tushar/spark-2.3.0-bin-hadoop2.7/python/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/appl_stock.csv',inferSchema=True,header=True)


# In[5]:


df.printSchema()


# In[6]:


df.show()


# In[12]:


df.head(2)[0]


# In[18]:


df.filter("Close < 500").select(['open','Low']).show()


# In[30]:


df.filter( (df['Close'] < 500) & (df['Low'] > 200)  ).select(['open','Low']).show()


# In[37]:


result = df.filter(df['Low'] == 197.16).collect()


# In[39]:


row = result[0]


# In[41]:


row.asDict()['Volume']


# In[ ]:




