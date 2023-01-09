import logging
import logging.config


### load the logging configuration file
logging.config.fileConfig(fname = '../util/logs_to_file.conf')

# Custom logger from configuration files
logger = logging.getLogger(__name__)


def load_files(spark, file_dir, file_format, header, inferSchema) :
    try:
        logger.info("load_file function has started !! ")
        if file_format == 'parquet' :
            df = spark. \
                read. \
                format(file_format). \
                load(file_dir)
        elif file_format == 'csv' :
            df = spark. \
                read. \
                format(file_format). \
                options(header = header). \
                options(inferSchema = inferSchema). \
                load(file_dir)
    except Exception as exception:
        logger.error("Error in the method - load_files(). Please check the Stack Trace. " + str(exception))
        raise
    else:
        logger.info(f"The input file {file_dir} is loaded to the dataframe. The load_files() has been created..")
    return df