# ####################################################Module Information################################################
#  Module Name         :   DATAOPS
#  Purpose             :   This module will  read JSON file and  sets the configurations for the  output file
#                           
#  Input Parameters    :   json files(pipeline,env)
#  Output Value        :   returns parameterised .py or .ipynb files
#  Pre-requisites      :
#  Created on          :   1 st may 2023
#  Created by          :   Surendra kumar
 # ######################################################################################################################
import json
import glue_job
import emr_job
from spark_template import template_creation
from script_file_generator import *

# Open a JSON file containing input and output  details
with open("pipeline.json", "r") as jsonfile:
    read_data = json.load(jsonfile)

    # Extract relevant information from the configuration data
    pipeline_name = read_data["pipeline_name"]
    connection = read_data["source"]["connector"]

# Open a JSON file containing environment variables   details
with open("env.json", "r") as jsonfile1:
    envjosn = json.load(jsonfile1)

    template = envjosn["template"]

    # Process the configuration data using a template and connection information
    processed_content = template_creation(read_data,template)

    # Save the processed content as an IPython notebook file if template is  databricks
    if template=="databricks":
     save_as_ipynb(template, pipeline_name, processed_content)
    # Save the processed content as an .py notebook file if template is  awsglue
    elif template=="awsglue":
     save_as_py(template, pipeline_name, processed_content)
     glue_job.run_gluejob(pipeline_name)
     # Save the processed content as an .py notebook file if template is  EMR
    elif template=="emr":
      save_as_py(template, pipeline_name, processed_content)
      emr_job.create_emr_job(envjosn)

    else:
        print("un identified template")













