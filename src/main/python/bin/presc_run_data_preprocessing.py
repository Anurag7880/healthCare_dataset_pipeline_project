import logging
import logging.config
from pyspark.sql.functions import upper, lit, regexp_extract, col, concat_ws, count, when, isnan, coalesce, avg, round
from pyspark.sql.window import Window


# Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)



def perform_data_clean(df1, df2):
    ### Clean df_city DataFrame:
    #1 Select only required Columns
    #2 Convert city, state and county fields to Upper Case
    try:
        logger.info(f"perform_data_clean() is started for df_city dataframe...")
        df_city_sel = df1.select(upper(df1.city).alias("city"),
                                 df1.state_id,
                                 upper(df1.state_name).alias("state_name"),
                                 upper(df1.county_name).alias("county_name"),
                                 df1.population,
                                 df1.zips)

        df_city_sel = df_city_sel.dropna(subset="zips")

    ### Clean df_fact DataFrame:
    #1 Select only required columns
    #2 Rename the columns
        logger.info(f"perform_data_clean() is started for df_fact dataframe...")
        df_fact_sel = df2.select(df2.npi.alias("presc_id"),df2.nppes_provider_last_org_name.alias("presc_lname"), \
                     df2.nppes_provider_first_name.alias("presc_fname"),df2.nppes_provider_city.alias("presc_city"), \
                     df2.nppes_provider_state.alias("presc_state"),df2.specialty_description.alias("presc_spclt"), df2.year_exp, \
                     df2.drug_name,df2.total_claim_count.alias("trx_cnt"),df2.total_day_supply, \
                     df2.total_drug_cost)
    #3 Add a country field 'USA'
        df_fact_sel = df_fact_sel.withColumn("country_name", lit("USA"))
    #4 Clean years_of_exp field
        pattern = '\d+'
        idx = 0
        df_fact_sel = df_fact_sel.withColumn('year_exp', regexp_extract(col('year_exp'), pattern, idx))
    #5 convert the years_of_exp datatype from string to Number/Integer
        df_fact_sel = df_fact_sel.withColumn('year_exp', col('year_exp').cast('int'))
    #6 Combine FirstName and LastName
        df_fact_sel = df_fact_sel.withColumn('presc_fullname', concat_ws(' ', 'presc_fname', 'presc_lname'))

        ### To google it later about lit function
        # df_fact_sel = df_fact_sel.withColumn('presc_fullname', col('presc_fname').lit('%'), col('presc_lname'))
        df_fact_sel = df_fact_sel.drop('presc_fname', 'presc_lname')
    #7 Check and clean all the Null/nan values
        # df_fact_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fact_sel.columns]).show()
    #8 Delete the records where the presc_id is null
        # df_fact_sel = df_fact_sel.dropna(subset="presc_id")       # we don't have any null values in presc_id column
    #9 Delete the records where the drug_name id null
        df_fact_sel = df_fact_sel.dropna(subset = "drug_name")

        ### For test whether any record having Null/Nan values
        # df_fact_sel.filter(df_fact_sel.drug_name.isNull()).select('presc_id', 'drug_name').show()
    #10 Impute TRX_CNT where it is null as avg of trx_cnt for the prescriber
        spec = Window.partitionBy("presc_id")
        df_fact_sel = df_fact_sel.withColumn('trx_cnt', coalesce("trx_cnt", round(avg("trx_cnt").over(spec))))

        ### For test whether any record having Null/Nan values
        # df_fact_sel.filter(df_fact_sel.trx_cnt.isNull()).select('presc_id', 'trx_cnt').show()
        df_fact_sel = df_fact_sel.withColumn('trx_cnt', col('trx_cnt').cast('integer'))

        ### Check and clean all the Null/nan values in console
        # df_fact_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fact_sel.columns]).show()
        # df_city_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_city_sel.columns]).show()

    except Exception as exp:
        logger.error("Error in the method - spark_curr_date(). Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("perform_data_clean() is completed...")
    return df_city_sel, df_fact_sel