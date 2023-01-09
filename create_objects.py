from pyspark.sql import SparkSession
import logging
import logging.config

# Load logging configuration file
logging.config.fileConfig(fname='../util/logs_to_file.conf')
logger = logging.getLogger(__name__)


def get_spark_object(envn, appName):
    try:
        logger.info(f"get_spark_object() has started and '{envn}' envn is used")
        if envn == 'TEST' :
            master = 'local'
        else:
            master = 'yarn'
        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()
    except NameError as exception:
        logger.error("\n\nNameError in the method - get_spark_object. Please check the Stack tree! "+ str(exception), exc_info=True)
        raise
    except Exception as exception:
        logger.error("Error in the method - get_spark_object. Please check the Stack tree! "+str(exception), exc_info=True)
    else:
        logger.info("SPARK OBJECT IS CREATED !!")
    return spark