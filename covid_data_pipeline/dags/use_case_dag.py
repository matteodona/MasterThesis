from airflow.decorators import dag, task
from datetime import datetime, timedelta
import utils

class DataCollection:
     def __init__(self, name, kw_args):
        self.name = name
        self.kw_args = kw_args




default_args = {'owner': 'matteo', 'depends_on_past': False, 'email_on_failure': False, 'email_on_retry': False, 'retries': 5, 'retry_delay_min': 5}




# Data Collections instantiation
country_wise_covid = DataCollection('country_wise_covid',{'path': 'data/country_wise_covid/country_wise_latest.csv'})
large_covid_data = DataCollection('large_covid_data',{'path': 'data/large_covid_data/owid-covid-data.csv'})


# Dag instantiation
@dag(
	dag_id = 'use_case_dag', 
	default_args = default_args, 
	schedule_interval = None, 
	description = 'Use Case dag', 
	start_date = datetime(2024,4,18)
)

# Tasks declaration
def define_pipeline():
   @task
   def task_1(large_covid_data, transformed_owid_covid_data):
        utils.transform_owid_data(large_covid_data, transformed_owid_covid_data)

   transformed_owid_covid_data = DataCollection('transformed_owid_covid_data', {'path': 'data/transformed_owid_covid_data'})
   task_1 = task_1(large_covid_data, transformed_owid_covid_data)
   

   @task
   def task_2(transformed_owid_covid_data, country_wise_covid, joined_data):
        utils.join_data(transformed_owid_covid_data, country_wise_covid, joined_data)

   joined_data = DataCollection('joined_data', {'path': 'data/joined_data'})
   task_2 = task_2(transformed_owid_covid_data, country_wise_covid, joined_data)

   @task
   def task_3(joined_data, best_recovery_countries):
        utils.get_best_countries(joined_data, best_recovery_countries)

   best_recovery_countries = DataCollection('best_recovery_countries', {'path': 'data/best_recovery_countries'})
   task_3 = task_3(joined_data, best_recovery_countries)

   @task
   def task_4(best_recovery_countries, db_data):
        utils.load_data_into_postgres(best_recovery_countries, db_data)

   db_data = DataCollection('db_data', {'path': 'data/db_data'})
   task_4 = task_4(best_recovery_countries, db_data)

     
# Tasks dependencies

   task_1 >> task_2
   task_2 >> task_3
   task_3 >> task_4

     

pipeline = define_pipeline()