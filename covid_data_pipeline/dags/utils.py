# Write here tasks callbacks
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, when
import os
import shutil

def transform_owid_data(large_covid_data, transformed_owid_covid_data):
    
	spark = SparkSession.builder.master('local') \
			.appName("COVID19-Data-Pipeline-Task1") \
			.getOrCreate()
      
	if check_output(transformed_owid_covid_data):
           
		owid_covid_df = spark.read.csv(large_covid_data.kw_args['path'], header=True, inferSchema=True)

		owid_covid_df_transformed = owid_covid_df.select(
		"location",
		"date",
		"total_cases",
		"new_cases",
		"total_deaths",
		"new_deaths").na.fill(0)
			
			
		owid_covid_agg = owid_covid_df_transformed.groupBy("location", "date").agg(
			{"total_cases": "sum", "new_cases": "sum", "total_deaths": "sum", "new_deaths": "sum"}
			).withColumnRenamed("sum(total_cases)", "total_cases")\
			.withColumnRenamed("sum(new_cases)", "new_cases")\
			.withColumnRenamed("sum(total_deaths)", "total_deaths")\
			.withColumnRenamed("sum(new_deaths)", "new_deaths")

		owid_covid_agg.write.mode('append').parquet(transformed_owid_covid_data.kw_args['path'])
            
	
	


	spark.stop()

	

def check_output(transformed_owid_covid_data):
    path = transformed_owid_covid_data.kw_args['path']
    
    if not os.path.exists(path) or not os.path.isdir(path):
        print("Provided path does not exist or is not a directory")
        return False

    operation_successful = True

    # iterate on files and subfolders
    for filename in os.listdir(path):
        file_path = os.path.join(path, filename)
        try:
            # remove subfolders using shutil.rmtree and files using con os.remove
            if os.path.isdir(file_path):
                shutil.rmtree(file_path)
            else:
                os.remove(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")
            operation_successful = False

    return operation_successful
        



def join_data(transformed_owid_covid_data, country_wise_covid, joined_data):
	spark = SparkSession.builder.master('local') \
    .appName("COVID19-Data-Pipeline-Task2") \
    .getOrCreate()
     
	if check_output(joined_data):

		country_wise_latest_df = spark.read.csv(country_wise_covid.kw_args['path'], header=True, inferSchema=True)
		transformed_owid_covid_df = spark.read.parquet(transformed_owid_covid_data.kw_args['path'])

		country_wise_latest_df = country_wise_latest_df.fillna(0)
		country_wise_latest_df = country_wise_latest_df.withColumnRenamed("Country/Region", "location")

		# clean data: fill null values and rename columns
		country_wise_latest_df = country_wise_latest_df.fillna(0)
		country_wise_latest_df = country_wise_latest_df.withColumnRenamed("Country/Region", "location")

		# compute new metrics: mortality and recovery rate 
		country_wise_latest_enriched = country_wise_latest_df.withColumn(
				"mortality_rate", when(col("Confirmed") > 0, col("Deaths") / col("Confirmed")).otherwise(0)
			).withColumn(
				"recovery_rate", when(col("Confirmed") > 0, col("Recovered") / col("Confirmed")).otherwise(0)
			)   

		# filter relevant columns
		country_wise_latest_selected = country_wise_latest_enriched.select(
			"location", "Confirmed", "Deaths", "Recovered", "mortality_rate", "recovery_rate"
		)

		# join the two datasets
		joined_df = transformed_owid_covid_df.join(country_wise_latest_selected, "location")
		df_unique = joined_df.dropDuplicates(['location'])
		df_unique.write.mode('append').parquet(joined_data.kw_args['path'])

	spark.stop()



def get_best_countries(joined_data, best_recovery_countries):
	spark = SparkSession.builder.master('local') \
    .appName("COVID19-Data-Pipeline-Task3") \
    .getOrCreate()
    
	if check_output(best_recovery_countries):

		joined_df = spark.read.parquet(joined_data.kw_args['path'])

		#identify the 10 countries with the best recovery rates
		best_recovery_countries = joined_df.select("location", "recovery_rate").orderBy(col("recovery_rate").desc()).limit(40)

		best_recovery_countries.write.mode('overwrite').csv('data/best_recovery_countries', header=True)



	spark.stop()



