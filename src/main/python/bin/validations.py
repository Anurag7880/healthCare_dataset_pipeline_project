import logging
import logging.config
import pandas

### Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def get_curr_date(spark):
    try:
        opDF = spark.sql(""" select current_date """)
        logger.info("Validate the Spark Object by printing Current Date - " + str(opDF.collect()))
    except NameError as exp:
        logger.error("NameError in the method - spark_curr_date(). Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    except Exception as exp:
        logger.error("Error in the method - spark_curr_date(). Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Spark Object is validated. Spark Object is ready to proceed.")

def df_count(df, dfName):
    try:
        logger.info(f"The DataFrame validation by count df_count() function is started for dataframe {dfName} ...")
        df_count = df.count()
        logger.info(f"The dataframe count is {df_count}.")
    except Exception as exp:
        logger.error("Error in the method - df_count(). Please check the stack trace. " + str(exp), exc_info= True)
        raise
    else:
        logger.info(f"The Dataframe validation by count df_count() is completed.")

def df_top10_rec(df, dfName):
    try:
        logger.info(f"The DataFrame validation by top 10 record df_top10_rec() is started for dataframe {dfName}...")
        logger.info(f"The DataFrame top 10 records are : ")
        df_pandas = df.limit(10).toPandas()
        # df_pandas = df.limit(20)         #In spark DF
        logger.info('\n \t' + df_pandas.to_string(index=False))
        # logger.info(df_pandas.show())     #In Spark DF
    except Exception as exp:
        logger.error("Error in the method - df_top10_rec(). Please check the stack trace." + str(exp), exc_info=True)
        raise
    else:
        logger.info("The dataframe validation by top 10 record df_top10_rec() is completed.")

def df_print_schema(df, dfName):
    try:
        logger.info(f"The dataframe schema validation for dataframe {dfName} ...")
        sch = df.schema.fields
        logger.info(f"The dataframe {dfName} schema is : ")
        for i in sch:
            logger.info(f"\t {i}")
    except Exception as exp:
        logger.error("Error in the method - df_print_schema(). Please check the stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("The dataframe schema validation is completed.")