import logging.config
import pandas

# Load logging configuration file
logging.config.fileConfig(fname='../util/logs_to_file.conf')
logger = logging.getLogger(__name__)

def get_current_date(spark):
        try:

                outputDF = spark.sql("""select current_date""")
                logger.info("\n\nValidate the spark object by printing current date" + str(outputDF.collect()))

        except NameError as exception:
                logger.error("\n\nNameError in the method - get_current_date(). Please check variable name in Validations.py " + str(exception), exc_info=True)
                raise

        except Exception as exception:
                logger.error("\n\nError in the method - get_current_date(). Please check variable name in Validations.py " + str(exception), exc_info=True)
        else:
                logger.info("\nSPARK OBJECT IS VALIDATED AND READY !")



def df_count(df,dfName):
    try:
        logger.info(f"The DataFrame Validation by count df_count() is started for Dataframe {dfName}...")
        df_count=df.count()
        logger.info(f"The DataFrame count is {df_count}.")
    except Exception as exception:
        logger.error("Error in the method - df_count(). Please check the Stack Trace. " + str(exception))
        raise
    else:
        logger.info(f"The DataFrame Validation by count df_count() is completed.")

def df_top10_records(df,dfName):
    try:
        logger.info(f"The DataFrame Validation by top 10 record df_top10_rec() is started for Dataframe {dfName}...")
        logger.info(f"The DataFrame top 10 records are:.")
        df_pandas=df.limit(10).toPandas()
        logger.info('\n \t'+ df_pandas.to_string(index=False))
    except Exception as exception:
        logger.error("Error in the method - df_top10_rec(). Please check the Stack Trace. " + str(exception))
        raise
    else:
        logger.info("The DataFrame Validation by top 10 record df_top10_rec() is completed.")

def df_print_schema(df,dfName):
    try:
        logger.info(f"The DataFrame Schema Validation for Dataframe {dfName}...")
        sch=df.schema.fields
        logger.info(f"The DataFrame {dfName} schema is: ")
        for i in sch:
            logger.info(f"\t{i}")
    except Exception as exception:
        logger.error("Error in the method - df_show_schema(). Please check the Stack Trace. " + str(exception))
        raise
    else:
        logger.info("The DataFrame Schema Validation is completed.")

