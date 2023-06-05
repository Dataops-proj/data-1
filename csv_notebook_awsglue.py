import hvac 
import pyspark 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 
from datetime import datetime 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col#Validation-notempty 
df = df.filter(~col('first_name').isNull()).limit(100)
df = df.filter(~col('last_name').isNull()).limit(100)

#Validation-custom 

if df.filter(df['company_name'].rlike('@')).count() > 0: 
      raise ValueError('Custom validation failed. Stopping processing.')  

elif df.filter(df['city'].rlike('@')).count() > 0: 
      raise ValueError('Custom validation failed. Stopping processing.')  

elif df.filter(df['address'].rlike('@')).count() > 0: 
      raise ValueError('Custom validation failed. Stopping processing.')  

#Transformations
else:
   df = df.withColumn('first_name', df['first_name'].cast('string'))
   df = df.withColumn('last_name', df['last_name'].cast('string'))
   df = df.withColumn('company_name', df['company_name'].cast('string'))
   df = df.withColumn('address', df['address'].cast('string'))
   df = df.withColumn('city', df['city'].cast('string'))
   df = df.withColumn('FULLNAME', concat("first_name", "last_name"))
df.write.mode('overwrite').format('csv').save('s3a://blue-buckets/two/')