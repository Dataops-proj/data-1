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
indian_timezone=pytz.timezone('Asia/Kolkata')
# configure the logging module to include the Indian time stamp 
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(message)s', datefmt='%Y-%m-%d %H:%M:%S %Z')
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
\ntry:\n\turl3 = base64.b64decode('aHR0cDovLzMuNi40MC4yMzE6ODIwMA==').decode('utf-8') \n\ttoken3 = base64.b64decode('cy54UGtIemZON2p4cHliNW9BR0JxeDRXSUM=').decode('utf-8')\n\tlogging.info('Starting data processing pipeline...')\n\n\tspark=SparkSession.builder.appName('DATA-OPS').getOrCreate()\n\tsc = spark.sparkContext\n\tlogging.info('Spark Context is created')\n
\n\tclient = hvac.Client(url=url3, token=token3)\n\ts_s3_credentials = client.read('kv/data/data/s3_credentials')['data']['data']\n\taccess_key = s_s3_credentials.get('access_key')\n\tsecret_key = s_s3_credentials.get('secret_key')\n\taws_region = 'ap-south-1'\n\tlogging.info('AWS S3 credentials authenticated from Hvac Vault')\n\n\t#Configure Spark to use AWS S3 credentials\n\n\tsc._jsc.hadoopConfiguration().set('fs.s3a.access.key', access_key)\n\tsc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', secret_key)\n\tsc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.' + aws_region + '.amazonaws.com')\n\n\t#Read data from S3 bucket\n\tdf = " \
                                "spark.read.format('csv').options(header='True').load('s3://red-buckets/us-500.csv')\n\tlogging.info('Data loaded from S3 bucket successfully')

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
	df.write.mode('overwrite').format('parquet').save('s3a://blue-buckets/CSV_to_Parquet_demo/')
	logging.info('Data written to S3 bucket successfully')
	logging.info('Data processing pipeline completed.')
except Exception as e:
	logging.error('Error occurred during data processing: {}'.format(str(e)))
#Move custom log file to S3 bucket 
s3 = boto3.client('s3', aws_access_key_id=s_s3_credentials.get('access_key'), aws_secret_access_key=s_s3_credentials.get('secret_key'),region_name=aws_region)

# Upload custom log file to S3
s3.upload_file('audit_logs.csv', 'blue-buckets', 'logs/audit_logs.csv')
logging.info('Custom log file saved to S3 successfully.')