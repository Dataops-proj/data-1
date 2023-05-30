import hvac 
import pyspark 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 
from datetime import datetime 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col
spark=SparkSession.builder.appName('DATA-OPS').getOrCreate()
sc = spark.sparkContext

client = hvac.Client(url='http://127.0.0.1:8200', token='hvs.sL1Obxm8SuQGR6KnzwUTH8JV')
s_s3_credentials = client.read('kv/data/data/source_s3_credentials')['data']['data']
access_key = s_s3_credentials.get('access_key')
secret_key = s_s3_credentials.get('secret_key')
aws_region = 'ap-south-1'

sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', access_key)
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', secret_key)
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.' + aws_region + '.amazonaws.com')

df = spark.read.format('csv').options(header='True').load('s3://red-buckets/us-500.csv')#Validation-notempty 
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