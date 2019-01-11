#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession.builder.appName('timestamp').getOrCreate()


# In[5]:


df = spark.read.csv('/home/tushar/spark-2.3.0-bin-hadoop2.7/python/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/appl_stock.csv',inferSchema=True,header=True)


# In[6]:


df.show()


# In[7]:


df.head(2)


# In[12]:


df.head()


# In[13]:


df.select(['Date','Open']).show()


# In[14]:


from pyspark.sql.functions import (dayofmonth,hour,dayofyear,month,year,weekofyear,format_number,date_format)


# In[15]:


df.select(dayofmonth(df['Date'])).show()


# In[16]:


df.select(year('Date')).show()


# In[17]:


df.show()


# In[19]:


#####
###
# WE ARE USING THE WITHCOLUMN TO CREATE NEW COLUMN BY FILTERING THE DATE COLUMN OF DATASET ON APPLYING FUNCTION YEAR


 newdf = df.withColumn("Years",year("Date"))


# In[26]:


# HERE WE ARE GROUPING BY THE YEARS AND FINDING THE MEAN!!!
#     THEN SELECTING THE YEARS AND AVG(CLOSE ) VALUES

result = newdf.groupBy("Years").mean().select(['Years','avg(close)'])


# In[27]:


result.show()


# In[31]:


result.select(['Years',(format_number("avg(close)",3).alias("Closing"))]).show()


# In[ ]:




