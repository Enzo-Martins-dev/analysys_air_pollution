from airflow.sdk import dag, task
from datetime import datetime, timedelta, timezone
import requests
import os
from dotenv import load_dotenv
import json
import pandas as pd 
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
import logging 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DIRETORIO_DADOS = "/opt/airflow/dados_persistentes/Dados"
DIRETORIO_SQL = "/opt/airflow/sql" 

POSTGRES_CONN_ID = 'postgres_conn'
NOME_TABELA_DB = "qualidade_ar_historico"
NOME_ARQUIVO_SQL = "create_table_air_pollution.sql"

DATA_INICIAL_FALLBACK = datetime(2020, 11, 27, tzinfo=timezone.utc)
MAXIMO_DIAS_POR_REQUISICAO = 365  

load_dotenv()
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
LAT = 40.7128
LNG = -74.0060
CIDADE = 'New York' 
BASE_URL_HISTORICO = 'http://api.openweathermap.org/data/2.5/air_pollution/history'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

@dag(
    dag_id='extrair_dados_openweather',
    default_args=default_args,
    schedule='30 14 * * *',
    start_date=datetime(2025, 11, 3),
    catchup=False,
    tags=['extracao', 'etl']
)
def extrair_dados_pipeline():
    
    @task(task_id="calculate_chunks")
    def calculate_extraction_chunks():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        data_maxima_raw = hook.get_first(f"""SELECT MAX(data_hora_utc) FROM {NOME_TABELA_DB};""")

        if data_maxima_raw and data_maxima_raw[0] is not None:
            data_maxima = data_maxima_raw[0].replace(tzinfo=timezone.utc)
        else:
            data_maxima = DATA_INICIAL_FALLBACK

        logging.info(f"Data máxima encontrada: {data_maxima}")

        chunks = []
        current_start = data_maxima
        tempo_fim_dt_atual = datetime.now(timezone.utc)
        
        while current_start < tempo_fim_dt_atual:
            current_end_limit = current_start + timedelta(days=MAXIMO_DIAS_POR_REQUISICAO)
            current_end = min(tempo_fim_dt_atual, current_end_limit)
            
            if current_end > current_start:
                chunks.append({
                    'start_unix': int(current_start.timestamp()),
                    'end_unix': int(current_end.timestamp()),
                    'start_dt_iso': current_start.isoformat(),
                    'end_dt_iso': current_end.isoformat(),
                })
            
            current_start = current_end
        
        logging.info(f"Número total de chunks a serem extraídos: {len(chunks)}")
        return chunks

    @task(task_id="extract_chunk")
    def extract_chunk(chunk: dict):
        start_unix = chunk['start_unix']
        end_unix = chunk['end_unix']
        
        if not OPENWEATHER_API_KEY:
            raise ValueError("OPENWEATHER_API_KEY não está definida")

        parametros = {
            'lat': LAT,
            'lon': LNG,
            'start': start_unix,
            'end': end_unix,
            'appid': OPENWEATHER_API_KEY
        }

        os.makedirs(DIRETORIO_DADOS, exist_ok=True)
        
        logging.info(f"Fazendo requisição para API OpenWeather: {BASE_URL_HISTORICO} de {chunk['start_dt_iso']} até {chunk['end_dt_iso']}")
        
        try:
            response = requests.get(BASE_URL_HISTORICO, params=parametros, timeout=300)
        except requests.exceptions.Timeout as e:
            logging.error(f"A requisição excedeu o tempo limite (Timeout): {e}")
            raise Exception(f"Timeout na requisição API: {e}")

        if response.status_code != 200:
            logging.error(f"Erro na requisição API: {response.status_code} - {response.text}")
            raise Exception(f"Erro na requisição API: {response.status_code} - {response.text}")
        else:
            logging.info("Requisição API bem-sucedida.")

        dados_json = response.json()
        dados_json['latitude'] = LAT
        dados_json['longitude'] = LNG
        dados_json['cidade'] = CIDADE 

        input_file_path = f'{DIRETORIO_DADOS}/openweather_{start_unix}_{end_unix}.json'
        with open(input_file_path, 'w', encoding='utf-8') as f:
            json.dump(dados_json, f, indent=4, ensure_ascii=False)

        return {'file_path': input_file_path, 'latitude': LAT, 'longitude': LNG, 'cidade': CIDADE}

    @task(task_id="setup_db")
    def setup_db():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        sql_file_path = os.path.join(DIRETORIO_SQL, NOME_ARQUIVO_SQL)
        
        if not os.path.exists(sql_file_path):
            raise FileNotFoundError(f"Arquivo SQL não encontrado: {sql_file_path}.")

        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()

        hook.run(sql_script)
        logging.info(f"Tabela '{NOME_TABELA_DB}' verificada/criada.")

    @task(task_id="transform_chunk")
    def transform_chunk(file_info: dict):
        input_file_path = file_info['file_path']

        output_file_name = os.path.basename(input_file_path).replace('.json', '_transformed.csv')
        output_file_path = os.path.join(DIRETORIO_DADOS, output_file_name)
        
        with open(input_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if 'list' not in data or not data['list']:
            logging.warning(f"Nenhum registro encontrado no arquivo JSON: {input_file_path}")
            return {'file_path': None, 'records_count': 0}

        df = pd.json_normalize(data['list'], sep='_')

        df.rename(columns={
            'dt': 'data_hora_utc',
            'main_aqi': 'indice_qualidade_ar',
            'components_co': 'co_monoxido_carbono',
            'components_no': 'no_monoxido_nitrogenio',
            'components_no2': 'no2_dioxido_nitrogenio',
            'components_o3': 'o3_ozonio',
            'components_so2': 'so2_dioxido_enxofre',
            'components_pm2_5': 'pm2_5_particulado',
            'components_pm10': 'pm10_particulado',
            'components_nh3': 'nh3_amonia',
        }, inplace=True)
        
        df['data_hora_utc'] = pd.to_datetime(df['data_hora_utc'], unit='s', utc=True)
        df['hora_int'] = df['data_hora_utc'].dt.hour
        df['latitude'] = data['latitude']
        df['longitude'] = data['longitude']
        df['cidade'] = data['cidade']
        df['indice_qualidade_ar'] = df['indice_qualidade_ar'].round().astype('Int64')

        colunas_ordenadas = [
            'data_hora_utc', 'hora_int', 'latitude', 'longitude', 'cidade', 
            'indice_qualidade_ar', 'co_monoxido_carbono', 'no_monoxido_nitrogenio', 
            'no2_dioxido_nitrogenio', 'o3_ozonio', 'so2_dioxido_enxofre', 
            'pm2_5_particulado', 'pm10_particulado', 'nh3_amonia'
        ]
        
        df = df[colunas_ordenadas]

        df.to_csv(output_file_path, index=False, encoding='utf-8')
        
        logging.info(f"Transformação concluída. Registros processados: {len(df)}")
        return {'file_path': output_file_path, 'records_count': len(df)}

    @task(task_id="consolidate_load")
    def consolidate_load(list_of_file_info: list):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        total_records_loaded = 0
        NOME_TABELA_STAGING = f"staging_{NOME_TABELA_DB}"

        hook.run(f"""
            CREATE TABLE IF NOT EXISTS {NOME_TABELA_STAGING} (
                LIKE {NOME_TABELA_DB} INCLUDING DEFAULTS
            );
            TRUNCATE TABLE {NOME_TABELA_STAGING};
        """)

        for file_info in list_of_file_info:
            file_path = file_info.get('file_path')
            records_count = file_info.get('records_count', 0)
            
            if not file_path or not os.path.exists(file_path) or records_count == 0:
                logging.warning(f"Pulando o carregamento para chunk sem arquivo/registros.")
                continue

            hook.copy_expert(
                f"COPY {NOME_TABELA_STAGING} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')",
                file_path
            )
            total_records_loaded += records_count
        
        if total_records_loaded > 0:
            insert_sql = f"""
                INSERT INTO {NOME_TABELA_DB}
                SELECT *
                FROM {NOME_TABELA_STAGING}
                ON CONFLICT (data_hora_utc, latitude, longitude) DO NOTHING;
            """
            hook.run(insert_sql)
            
            hook.run(f"TRUNCATE TABLE {NOME_TABELA_STAGING};")
            
            logging.info(f"Load finalizado. Total de {total_records_loaded} registros carregados em '{NOME_TABELA_DB}'.")
        else:
            logging.info("Nenhum registro novo para carregar após processar todos os chunks.")

        return {'loaded_records': total_records_loaded}


    setup_db_task = setup_db()
    
    chunks_list = calculate_extraction_chunks()
    
    extraction_results = extract_chunk.expand(chunk=chunks_list)
    
    transform_results = transform_chunk.expand(file_info=extraction_results)
    
    load_task = consolidate_load(transform_results)

    setup_db_task
    chunks_list >> extraction_results
    
    [setup_db_task, transform_results] >> load_task

dag = extrair_dados_pipeline()