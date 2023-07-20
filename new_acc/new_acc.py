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

	spark=SparkSession.builder.config("spark.jars.packages", "org.postgresql:postgresql:42.6.0").appName('DATA-OPS').getOrCreate()
	sc = spark.sparkContext
	logging.info('Spark Context is created')

	url_dcp = base64.b64decode('aHR0cDovLzU0LjE4NC43Ny4xNDY6ODIwMA==').decode('utf-8')
	token_dcp = base64.b64decode('cy5WbkNERVNOc1d3S25JQkF0T1JHNmJKaUQ=').decode('utf-8')

	client = hvac.Client(url=url_dcp, token=token_dcp)
	s_s3_credentials = client.read('kv/data/data/S3_credentials')['data']['data']
	database_cred_s = client.read('kv/data/data/s_database')['data']['data']
	database_cred_t = client.read('kv/data/data/t_database')['data']['data']
	username_s = database_cred_s.get('username')
	password_s = database_cred_s.get('password')
	username_t = database_cred_t.get('username')
	password_t = database_cred_t.get('password')
	access_key = s_s3_credentials.get('aws_access_key_id')
	secret_key = s_s3_credentials.get('aws_secret_access_key')
	aws_region = 'ap-south-1'
	logging.info('AWS S3 credentials and database authenticated from Hvac Vault')
	df = spark.read.format('jdbc').option('url','jdbc:postgresql://database-1.crlupmqhrzfz.ap-south-1.rds.amazonaws.com:5432/postgres.public').option('query', '(SELECT * FROM us_500 ) as us_500').option('user', username_s).option('password', password_s).option('driver', 'org.postgresql.Driver').load()

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

	#writing the dataframe to RDS 
	df.write.format('jdbc').mode(overwrite).option('url', jdbc:postgresql://dataops-db.cr5bcibr4zvb.ap-south-1.rds.amazonaws.com:5432/postgres.public).option('dbtable', us1).option('user', username_t).option('password', password_t).save()

	logging.info('Data written to RDS successfully')
	logging.info('Data processing pipeline completed.')

	#Move custom log file to S3 bucket 
	s3 = boto3.client('s3', aws_access_key_id= access_key, aws_secret_access_key= secret_key,region_name=aws_region)

	# Upload custom log file to S3
	s3.upload_file('audit_logs.csv', 'dataops-source-bucket', 'logs/audit_logs.csv')
	logging.info('Custom log file saved to S3 successfully.')

except Exception as e:
	logging.error('Error occurred during data processing: {}'.format(str(e)))