#bibliotecas
import pandas as pd
from datetime import datetime
from great_expectations.validator.validator import Validator
from great_expectations.execution_engine.pandas_execution_engine import PandasExecutionEngine
from great_expectations.core.batch import Batch, BatchMarkers, BatchSpec
import great_expectations as ge
import warnings

#bloquear avisos do great expectations
warnings.filterwarnings(
    "ignore",
    message="`result_format` configured at the Validator-level will not be persisted.*",
    category=UserWarning
)

def transformar_prata(dataset):

    # As informações que eu quero então truncadas dentro de um dicionário dentro de uma das colunas do dataframe
    dias_df = dataset.explode('forecast.forecastday')
    # normalizar os dados da coluna forecast.forecastday
    dias_norm = pd.json_normalize(dias_df['forecast.forecastday'])
    # regatar a lista dentro da coluna hour dentro do dataframe forecast.forecastday
    horas_explod = dias_norm.explode('hour')
    # transformar os dados da coluna hour no dataframe principal
    dataset= pd.json_normalize(horas_explod['hour'])

    #remover métricas que não se aplicam apenas quando elas existem na tabela
    colunas_remover = [
        'temp_f','wind_mph','pressure_in','precip_in','feelslike_f','windchill_f',
        'heatindex_f','dewpoint_f','vis_miles','gust_mph','short_rad','diff_rad',
        'dni','gti','condition.text','condition.icon','condition.code'
    ]
    dataset = dataset.drop(columns=[c for c in colunas_remover if c in dataset.columns])
        
    #alterar os nomes das colunas
    novos_nomes = {
        'time_epoch': 'epoca_tempo',
        'time': 'hora',
        'temp_c': 'temperatura_c',
        'is_day': 'é_dia',
        'wind_kph': 'vento_kph',
        'wind_degree': 'grau_vento',
        'wind_dir': 'direção_vento',
        'pressure_mb': 'pressão_mb',
        'precip_mm': 'precipitação_mm',
        'snow_cm': 'neve_cm',
        'humidity': 'umidade',
        'cloud': 'nuvens',
        'feelslike_c': 'sensação_c',
        'windchill_c': 'frio_vento_c',
        'heatindex_c': 'índice_calor_c',
        'dewpoint_c': 'ponto_orvalho_c',
        'will_it_rain': 'vai_chover',
        'chance_of_rain': 'chance_chuva',
        'will_it_snow': 'vai_nevar',
        'chance_of_snow': 'chance_neve',
        'vis_km': 'visibilidade_km',
        'gust_kph': 'rajada_kph',
        'uv': 'índice_uv'
    }

    dataset.rename(columns=novos_nomes, inplace=True)

    #transformar coluna em datetime
    dataset['hora'] = pd.to_datetime(dataset['hora'])

    #cria contexto para validação
    context = ge.get_context()

    #context.create_expectation_suite("validacao_prata", overwrite_existing=True)

    validator = context.get_validator(
        batch_data=dataset,
        #expectation_suite_name="validacao_prata"
        )



    #validações do dataframe hoje
    validator.expect_column_values_to_be_between('temperatura_c', min_value=-20, max_value=50)
    validator.expect_column_values_to_be_between('sensação_c', min_value=-20, max_value=50)
    validator.expect_column_values_to_be_between('frio_vento_c', min_value=-20, max_value=50)
    validator.expect_column_values_to_be_between('índice_calor_c', min_value=-20, max_value=50)
    validator.expect_column_values_to_be_between('ponto_orvalho_c', min_value=-20, max_value=50)
    validator.expect_column_values_to_be_in_set('é_dia', [0, 1])
    validator.expect_column_values_to_be_in_set('vai_chover', [0, 1])
    validator.expect_column_values_to_be_in_set('vai_nevar', [0, 1])
    validator.expect_column_values_to_be_between('índice_uv', min_value=0)
    validator.expect_column_values_to_not_be_null('hora')

    results = validator.validate()

    #metricas
    falhas = sum(not r.success for r in results.results)
    print(f"Falhas: {falhas}")

    return dataset, results
