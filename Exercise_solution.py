#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession.builder.appName('exercise').getOrCreate()


# In[3]:


df = spark.read.csv('/home/tushar/spark-2.3.0-bin-hadoop2.7/python/Python-and-Spark-for-Big-Data-master/Spark_DataFrame_Project_Exercise/walmart_stock.csv',inferSchema=True,header=True)


# In[4]:


df.describe()


# In[5]:


df.describe().show()


# In[6]:


df.columns


# In[7]:


df.printSchema()


# In[8]:


df.head(5)


# In[102]:


from pyspark.sql.functions import format_number,mean


# In[10]:


df.show()


# In[14]:


showd = df.select(format_number('Adj Close',3).alias('Adj Close'))


# In[18]:


df.describe().show()


# In[47]:


from pyspark.sql.functions import dayofmonth,dayofyear,year


# In[23]:


df.select(dayofmonth("Date"),'Volume',).show()


# In[33]:


#df.select('High Price','Volume').divide().show()


# In[39]:


df.select(dayofmonth('Date'),'High').show()


# In[40]:


df.groupBy('High').max().show()d


# In[53]:


data1 = df.select(year('Date'))


# In[54]:


data2 = df.select('High').show()


# In[60]:


result = df.describe()


# In[75]:


result.select(result['summary'],format_number(result['Open'].cast('float'),2).alias('Open'),
              format_number(result['High'].cast('float'),2).alias('High'),
format_number(result['Low'].cast('float'),2).alias('Low'),
format_number(result['Close'].cast('float'),2).alias('close'),
result['Volume'].cast('int').alias('Volume')).show()


# In[78]:


#ratio of high price vs volume of stock traded
new1 =df.withColumn("HV Ratio", df['High']/df['Volume'])


# In[79]:


new1.select('HV Ratio').show()


# In[100]:


df.orderBy(df['High'].desc()).head(1)[0][0]


# In[103]:


df.select(mean('Close')).show()


# In[126]:


from pyspark.sql.functions import max,min,count,month


# In[127]:


df.select(max('Volume'),min("Volume")).show()


# In[128]:


df.filter('Close < 60').count()


# In[129]:


(df.filter(df['High']>80).count() / df.count() ) * 100


# In[130]:


yeardf =  df.withColumn("Year",year(df['Date']))


# In[131]:


max_df = yeardf.groupBy('Year').max()


# In[125]:


max_df.select('Year','max(High)').show()


# In[148]:


#####
##################################
            # AVERAGE CLOSE FOR EACH CALENDAR MONTH


date1 = df.withColumn("Month",month('Date'))


# In[139]:





# In[146]:


monthavg = date1.select(["Month","Close"]).groupBy("Month").mean()


# In[147]:


monthavg.select("Month",'avg(Close)').orderBy('Month').show()


# In[ ]:




