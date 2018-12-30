#!/usr/bin/env python
# coding: utf-8

# In[10]:


# df.na. (methods) are used to drop the items present in data

from pyspark.sql import SparkSession


# In[11]:


spark = SparkSession.builder.appName('misssing').getOrCreate()


# In[12]:


df = spark.read.csv('/home/tushar/spark-2.3.0-bin-hadoop2.7/python/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/ContainsNull.csv',inferSchema=True,header=True)


# In[13]:


df.show()


# In[14]:


df.describe().show()


# In[15]:


df.columns


# In[16]:


df.printSchema()


# In[17]:


df.show()


# In[19]:


df.na.drop().show()


# In[23]:


# drops those rows which have null values =2 

df.na.drop(thresh=2).show()


# In[27]:


(df.na.drop(how='all')).show()


# In[29]:


#drop value for null in sales columnn

df.na.drop(subset=['Sales']).show()


# In[32]:


#########################################
# TO FILL UP THE DATA WHERE NULL IS PRESENT

df.na.fill('No name',subset=('Name')).show()


# In[33]:


df.na.fill(2,subset=('Sales')).show()


# In[34]:



# WE ARE FILLING THE NUMERICAL DATA IN SALES COLUMN WITH THE MEAN VALUE OF THE SALES OF THE DATASET
from pyspark.sql.functions import mean


# In[36]:


sales_mean = df.select(mean('Sales')).collect()
                


# In[43]:


mean_sales = sales_mean[0][0]


# In[44]:


mean_sales


# In[46]:


df.na.fill(mean_sales,subset=('Sales')).show()


# In[ ]:




