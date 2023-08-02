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

	url_dcp = base64.b64decode('aHR0cDovLzU0LjE4NC43Ny4xNDY6ODIwMA==').decode('utf-8')
	token_dcp = base64.b64decode('cy5WbkNERVNOc1d3S25JQkF0T1JHNmJKaUQ=').decode('utf-8')

	client = hvac.Client(url=url_dcp, token=token_dcp)
	s_s3_credentials = client.read('dataops/data/data/dataops-source-bucket')['data']['data']
	s_access_key = s_s3_credentials.get('aws_access_key_id')
	s_secret_key = s_s3_credentials.get('aws_secret_access_key')
	logging.info('AWS S3 credentials authenticated from Hvac Vault')

	#Configure Spark to use AWS S3 credentials

	sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', s_access_key)
	sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', s_secret_key)

	#Read data from S3 bucket
	df = spark.read.format('csv').options(header='True').load('s3://dataops-source-bucket/us-500.csv')
	logging.info('The file us-500.csv loaded from S3 bucket successfully')

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

	# Get database credentials from Vault
	database_credentials_target = client.secrets.kv.v2.read_secret(mount_point = 'dataops', path='data/dataopsmysql.cr5bcibr4zvb.ap-south-1.rds.amazonaws.com')['data']['data']
	username_t = database_credentials_target.get('username')
	password_t = database_credentials_target.get('password')

	#Writing the dataframe to database
	df.write.format('jdbc').mode('overwrite').option('url', 'jdbc:postgresql://dataopsmysql.cr5bcibr4zvb.ap-south-1.rds.amazonaws.com:3306/postgres').option('driver', 'org.postgresql.Driver').option('dbtable', 's3_to_postgresql_test_august_2').option('user', username_t).option('password', password_t).save()

	logging.info('Data written to RDS successfully')
	logging.info('Data processing pipeline completed.')

	logging.info('Data Transformation completed successfully')

	# Move custom log file to S3 bucket
	logs_credentials = client.secrets.kv.v2.read_secret(mount_point = 'kv', path = 'data/{}')['data']['data']
	access_key = logs_credentials.get('aws_access_key_id')
	secret_key = logs_credentials.get('aws_secret_access_key')
	aws_region = 'ap-south-1'
	s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=aws_region)

	# Upload custom log file to S3
	s3.upload_file('audit_logs.csv', 'dataops-source-bucket', 'logs/audit_logs.csv')
	logging.info('Custom log file saved to S3 successfully.')

except Exception as e:
	logging.error('Error occurred during data processing: {}'.format(str(e)))