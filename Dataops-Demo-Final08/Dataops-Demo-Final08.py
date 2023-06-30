import hvac 
import pytz 
import boto3 
import logging 
import pyspark 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 
from datetime import datetime 
from pyspark.sql import SparkSession 
import base64

#Configure logging
indian_timezone=pytz.timezone("Asia/Kolkata")
# configure the logging module to include the Indian time stamp 
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s, %(levelname)s, %(message)s", datefmt="%Y-%m-%d %H:%M:%S %Z")
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
file_handler = logging.FileHandler("audit_logs.csv")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

with open("audit_logs.csv", "r+") as f:
	content = f.read()
	f.seek(0, 0)
	f.write("TimeStamp, Log_level, Log_Message\n" + content)
try:
	logging.info('Starting data processing pipeline...')

	spark=SparkSession.builder.appName('DATA-OPS').getOrCreate()
	sc = spark.sparkContext
	logging.info('Spark Context is created')

	url_dcp = base64.b64decode('aHR0cDovLzMuNi40MC4yMzE6ODIwMA==').decode('utf-8')
	token_dcp = base64.b64decode('cy54UGtIemZON2p4cHliNW9BR0JxeDRXSUM=').decode('utf-8')

	client = hvac.Client(url=url_dcp, token=token_dcp)
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

	#Get the number of rows
	num_rows = df.count()
	logging.info(f'Number of rows in the file: {num_rows}')

	# Get the number of columns
	num_cols = len(df.schema.fields)
	logging.info(f'Number of columns in the file: {num_cols}')

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

	logging.info('Data Transformation completed successfully')

	#writing the dataframe to s3 bucket
	df.write.mode('overwrite').format('parquet').save('s3a://blue-buckets/Dataops-Demo-Final-4/')

	logging.info('Data written to S3 bucket successfully')
	logging.info('Data processing pipeline completed.')
except Exception as e:
	logging.error('Error occurred during data processing: {}'.format(str(e)))
#Move custom log file to S3 bucket 
s3 = boto3.client('s3', aws_access_key_id=s_s3_credentials.get('access_key'), aws_secret_access_key=s_s3_credentials.get('secret_key'),region_name=aws_region)

# Upload custom log file to S3
s3.upload_file('audit_logs.csv', 'blue-buckets', 'logs/audit_logs.csv')
logging.info('Custom log file saved to S3 successfully.')