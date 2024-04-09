from pyspark.sql.functions import upper, countDistinct, sum, split, dense_rank, col
from pyspark.sql.window import Window
from udfs import column_split_cnt

import logging
import logging.config

# Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')
# Get the custom logger from configuration file
logger = logging.getLogger(__name__)


def city_report(df_city_sel, df_fact_sel):
    try:
        logger.info(f"Tranform - city_report() is started ...")
        """
            # City Report:
               Transform Logics:
               1. Calculate the Number of zips in each city.
               2. Calculate the number of distinct Prescribers assigned for each City.
               3. Calculate total TRX_CNT prescribed for each city.
               4. Do not report a city in the final report if no prescriber is assigned to it.
    
            Layout:
               City Name
               State Name
               County Name
               City Population
               Number of Zips
               Prescriber Counts
               Total Trx counts
    
        """

        df_city_split = df_city_sel.withColumn("zip_count", column_split_cnt(df_city_sel.zips))
        df_fact_grp = df_fact_sel.groupBy("presc_state", "presc_city").agg(countDistinct("presc_id").alias("presc_counts"), sum("trx_cnt").alias("trx_counts"))
        df_city_join = df_city_split.join(df_fact_grp, (df_city_split.state_id == df_fact_grp.presc_state) & (df_city_split.city == df_fact_grp.presc_city), 'inner')
        df_city_final = df_city_join.select("city", "state_name", "county_name", "population", "zip_count", "trx_counts", "presc_counts")

    except Exception as exp:
        logger.error("Error in method - city_report(). Please check the Stack trace." + str(exp), exc_info=True)
        raise
    else:
        logger.info("Transform - city_report() is completed ...")
    return df_city_final


def top_5_prescribers(df_fact_sel):
    """
    # prescriber report
    Top 5 Prescribers with highest trx_cnt per each state.
        Consider the prescribers only from 20 to 50 years of experience.
        Layout:
          Prescriber ID
          Prescriber Full Name
          Prescriber State
          Prescriber Country
          Prescriber Years of Experience
          Total TRX Count
          Total Days Supply
          Total Drug Cost
    """
    try:
        logger.info("Transform - top_5_prescribers() is started ...")
        df_fact_filter = df_fact_sel.select("presc_id", "presc_fullname", "presc_state", "country_name", "year_exp", \
                                            "trx_cnt", "total_day_supply", "total_drug_cost") \
                                    .filter((df_fact_sel.year_exp >= 20) & (df_fact_sel.year_exp <= 50))
        ### For test purpose on console
        #df_fact_filter.show(10)
        spec = Window.partitionBy("presc_state").orderBy(col("trx_cnt").desc())
        df_rank = df_fact_filter.withColumn("dense_rank", dense_rank().over(spec))
        ###For test purpose on console
        #df_rank.show(10)
        df_presc_final = df_rank.filter(df_rank.dense_rank <= 5).select("presc_id", "presc_fullname", "presc_state", \
                                                                        "country_name", "year_exp", "trx_cnt", \
                                                                        "total_day_supply", "total_drug_cost")

    except Exception as exp:
        logger.error("Error in the method - top_5_prescribers(). Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Transform - top_5_prescribers() is completed ...")
    return df_presc_final






