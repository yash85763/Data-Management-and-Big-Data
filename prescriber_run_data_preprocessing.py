import logging
import logging.config
from pyspark.sql.functions import upper, lit, regexp_extract, col, concat_ws, count, isnan, when, avg, round, coalesce
from pyspark.sql.window import Window

# load logging configuration file
logging.config.fileConfig(fname = '../util/logs_to_file.conf')
logger = logging.getLogger(__name__)

def data_cleaning(df1, df2):
    ## Clean df_city dataframe
    #1. Select only required columns
    #2. Convert City, State and County fields to upper case
    try:
        logger.info(f"data_cleaning() has started..")
        df_city_sel = df1.select(upper(df1.city).alias("city"),
                                 df1.state_id,
                                 upper(df1.state_name).alias("state_name"),
                                 upper(df1.county_name).alias("county_name"),
                                 df1.population,
                                 df1.zips)
        ## Clean df_city dataframe:
        #1. Select only required columns
        logger.info(f"perform_data_clean() is started for df_fact dataframe...")
        df_fact_sel = df2.select(df2.npi.alias("presc_id"), df2.nppes_provider_last_org_name.alias("presc_lname"), \
                                 df2.nppes_provider_first_name.alias("presc_fname"), \
                                 df2.nppes_provider_city.alias("presc_city"), \
                                 df2.nppes_provider_state.alias("presc_state"), \
                                 df2.specialty_description.alias("presc_spclt"), df2.years_of_exp, \
                                 df2.drug_name, df2.total_claim_count.alias("trx_cnt"), df2.total_day_supply, \
                                 df2.total_drug_cost)
        # 3 Add a Country Field 'USA'
        df_fact_sel = df_fact_sel.withColumn("country_name", lit("USA"))
        # 4 Clean years_of_exp field
        pattern = '\d+'
        idx = 0
        df_fact_sel = df_fact_sel.withColumn("years_of_exp", regexp_extract(col("years_of_exp"), pattern, idx))
        # 5 Convert the yearS_of_exp datatype from string to Number
        df_fact_sel = df_fact_sel.withColumn("years_of_exp", col("years_of_exp").cast("int"))
        # 6 Combine First Name and Last Name
        df_fact_sel = df_fact_sel.withColumn("presc_fullname", concat_ws(" ", "presc_fname", "presc_lname"))
        df_fact_sel = df_fact_sel.drop("presc_fname", "presc_lname")
        #7.Check and clean all null values
        #df_fact_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fact_sel.columns]).show()
        # 8 Delete the records where the PRESC_ID, Presc_city, Pres_state, Presc_spclt is NULL
        df_fact_sel = df_fact_sel.dropna(subset="presc_id")
        df_fact_sel = df_fact_sel.dropna(subset="presc_city")
        df_fact_sel = df_fact_sel.dropna(subset="presc_state")
        df_fact_sel = df_fact_sel.dropna(subset="presc_spclt")

        # 9 Delete the records where the DRUG_NAME, years_of_exp, total_day_supply, total_drug_cost is NULL
        df_fact_sel = df_fact_sel.dropna(subset="drug_name")
        df_fact_sel = df_fact_sel.dropna(subset="years_of_exp")
        df_fact_sel = df_fact_sel.dropna(subset="total_day_supply")
        df_fact_sel = df_fact_sel.dropna(subset="total_drug_cost")

        # 10 Impute TRX_CNT where it is null as avg of trx_cnt for that prescriber
        spec = Window.partitionBy("presc_id")
        df_fact_sel = df_fact_sel.withColumn('trx_cnt', coalesce("trx_cnt", round(avg("trx_cnt").over(spec))))
        df_fact_sel = df_fact_sel.withColumn("trx_cnt", col("trx_cnt").cast('integer'))

        # Check and clean all the Null/Nan Values
        df_fact_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fact_sel.columns]).show()

    except Exception as exception:
        logger.error("Error in the method - data_cleaning(). Please check the stack trace. " + str(exception), exc_info=True)
        raise
    else:
        logger.info("data_cleaning has completed.")
    return df_city_sel, df_fact_sel

