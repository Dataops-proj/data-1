import hvac 
import pytz 
import boto3 
import logging 
import pyspark 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 
from datetime import datetime 
from pyspark.sql import SparkSession

#Configure logging
indian_timezone=pytz.timezone('Asia/Kolkata')
# configure the logging module to include the Indian time stamp 
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S %Z')
#Time_converter_function
def converter(timestamp):
	utc_time = datetime.utcfromtimestamp(timestamp)
	local_time = pytz.utc.localize(utc_time).astimezone(indian_timezone)
	return local_time.timetuple()
formatter.converter = converter

handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
#logging_file
file_handler = logging.FileHandler('audit_logs.csv')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

try:
	logging.info('Starting data processing pipeline...')

	spark=SparkSession.builder.appName('DATA-OPS').getOrCreate()
	sc = spark.sparkContext
	logging.info('Spark Context is created')

	client = hvac.Client(url='http://3.6.40.231:8200', token='s.xPkHzfN7jxpyb5oAGBqx4WIC')
	s_s3_credentials = client.read('kv/data/data/s3_credentials')['data']['data']
	access_key = s_s3_credentials.get('access_key')
	secret_key = s_s3_credentials.get('secret_key')
	aws_region = 'ap-south-1'
	logging.info('AWS S3 credentials authenticated from Hvac Vault')

	#Configure Spark to use AWS S3 credentials

	sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', access_key)
	sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', secret_key)
	sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.' + aws_region + '.amazonaws.com')

	#Read data from S3 bucket
	df = spark.read.format('csv').options(header='True').load('s3://red-buckets/us-500.csv')
	logging.info('Data loaded from S3 bucket successfully')

	#Validation-notempty
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
	df.write.mode('overwrite').format('parquet').save('s3a://blue-buckets/one/')
	logging.info('Data written to S3 bucket successfully')
	logging.info('Data processing pipeline completed.')
except Exception as e:
	logging.error('Error occurred during data processing: {}'.format(str(e)))
#Move custom log file to S3 bucket 
s3 = boto3.client('s3', aws_access_key_id=s_s3_credentials.get('access_key'), aws_secret_access_key=s_s3_credentials.get('secret_key'),region_name=aws_region)

# Upload custom log file to S3
s3.upload_file('audit_logs.csv', 'blue-buckets', 'logs/audit_logs.csv')
logging.info('Custom log file saved to S3 successfully.')