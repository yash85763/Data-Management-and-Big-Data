### Importing all the required Modules
import os

import get_all_variables as GAV
from create_objects import get_spark_object as GSO
from Validations import get_current_date as GCD
from Validations import df_count, df_top10_records, df_print_schema
import sys
import logging
import logging.config
from run_prescriber_data_ingest import load_files
from prescriber_run_data_preprocessing import data_cleaning
from prescriber_run_data_transform import city_report, top_5_Prescribers

### load the logging configuration file
logging.config.fileConfig(fname = '../util/logs_to_file.conf')

def main():

    ### To get all the variables - these variables can be local and environment variables
    try:
        logging.info("main() has started")
        spark = GSO(GAV.envn, GAV.appName)
        GCD(spark)

        ### Initiate run_prescriber_data_ingest Script
        # load the City file
        for file in os.listdir(GAV.staging_dim_city):
            print("File is "+ file)
            file_dir = GAV.staging_dim_city + '\\' + file
            print(file_dir)
            if file.split('.')[1] == 'csv' :
                file_format = 'csv'
                header = GAV.header
                inferSchema = GAV.inferSchema
            elif file.split('.')[1] == 'parquet' :
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

        df_city = load_files(spark = spark, file_dir = file_dir, file_format = file_format, header = header, inferSchema = inferSchema)

        ## Validate data ingest script for city dimension dataframe
        df_count(df_city, 'df_city')
        df_top10_records(df_city, 'df_city')


        # load the Prescriber fact file
        # Load the Prescriber Fact File
        for file in os.listdir(GAV.staging_fact):
            print("File is " + file)
            file_dir = GAV.staging_fact + '\\' + file
            print(file_dir)
            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = GAV.header
                inferSchema = GAV.inferSchema
            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)
        # Validate
        df_count(df_fact, 'df_fact')
        df_top10_records(df_fact, 'df_fact')
        # Setting ip Logging Configuration Mechanism
        # Set up Error handling



        ### Initiating prescriber_run_data_preprocessing script
        # Perform data Cleaning Operations
        df1_city_sel, df_fact_sel = data_cleaning(df_city, df_fact)
        # Validate
        df_top10_records(df1_city_sel, 'df1_city_sel')
        df_top10_records(df_fact_sel, 'df_fact_sel')
        df_print_schema(df_fact_sel, 'df_fact_sel')

        ### Initiate prescriber_run_data_transform script
        df_city_final = city_report(df1_city_sel, df_fact_sel)
        df_presc_final = top_5_Prescribers(df_fact_sel)

        #Validate the above dataframe
        df_top10_records(df_city_final, 'df_city_final')
        df_print_schema(df_city_final, 'df_city_final')
        df_top10_records(df_presc_final, 'df_presc_final')
        df_print_schema(df_presc_final, 'df_presc_final')



        ### Initiating run_data_extraction Script
        # Validation of processes
        # Setting ip Logging Configuration Mechanism
        # Setting up Error handling
        logging.info("run_prescriber_pipeline.py has COMPLETED !! ")
    except Exception as exception:
        logging.error("\nError occured in the main python script. Please check the Stack Trace to go to the respective module and try fixing it. " + str(exception), exc_info=True)
        sys.exit(1)
    ### end of first part

if __name__ == "__main__" :
    logging.info(" run_prescriber_pipeline has started...")
    main()