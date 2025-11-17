# from __future__ import annotations

# from importlib.abc import Loader
# from logging import Logger

# import pendulum

# from airflow.models.dag import DAG
# from airflow.operators.python import PythonOperator

# from trabalho_final.src.tasks.extractor import Extractor
# from trabalho_final.src.tasks.transformer import Transformer

# log = Logger("dags.selic")

# def _fetch_selic_data(date, **kwargs):
#     log.logger.info("Starting ETL pipeline")

#     extractor = Extractor()
#     transformer = Transformer()
#     loader = Loader()

#     df = extractor.collect_news()
#     loader.load_news(df)

#     df = extractor.collect_monetary_history()
#     loader.load_monetary_history(df)

#     df = extractor.collect_selic_history()
#     df_t = transformer.bcb_selic_to_history_series(df)
#     loader.load_selic_history(df_t)

#     log.logger.info("ETL pipeline finished successfully!")

#     log.logger.info("Closing dependencies...")
#     loader.close()

# with DAG(
#     dag_id="selic_dag",
#     start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
#     catchup=False,
#     schedule="@daily",
#     tags=["finance"],
# ) as dag:
     
#     date_str = "{{ ds }}"

#     fetch_selic_data_task = PythonOperator(
#         task_id="fetch_selic_data",
#         python_callable=_fetch_selic_data,
#         op_kwargs={"date": date_str},
#     )

