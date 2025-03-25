import requests
import pandas as pd
from bs4 import BeautifulSoup
import pyarrow.parquet as pq
import pyarrow as pa
import boto3
from datetime import datetime
import json

# Configuração AWS
S3_BUCKET = "bovespa-data-pipeline"
RAW_PATH = "raw/{}/dados.parquet".format(datetime.today().strftime("%Y-%m-%d"))
REFINED_PATH = "refined/{}/".format(datetime.today().strftime("%Y-%m-%d"))
LAMBDA_FUNCTION_NAME = "trigger-glue-job"
GLUE_JOB_NAME = "bovespa-etl-job"

# URL da B3
URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"

# Função para extrair dados da B3
def scrape_b3():
    response = requests.get(URL)
    soup = BeautifulSoup(response.text, 'html.parser')
    tabela = soup.find("table")
    
    data_list = []
    if tabela:
        for linha in tabela.find_all("tr")[1:]:
            colunas = linha.find_all("td")
            if len(colunas) > 1:
                data_list.append({
                    "nome_acao": colunas[0].text.strip(),
                    "preco": float(colunas[1].text.strip().replace(",", ".")),
                    "variacao": float(colunas[2].text.strip().replace(",", ".")),
                    "data_pregao": datetime.today().strftime("%Y-%m-%d")
                })
    return pd.DataFrame(data_list)

# Salvar no S3
def save_to_s3(df, path):
    tabela_parquet = pa.Table.from_pandas(df)
    pq.write_table(tabela_parquet, "dados.parquet")
    s3 = boto3.client("s3")
    s3.upload_file("dados.parquet", S3_BUCKET, path)
    print(f"Arquivo enviado para s3://{S3_BUCKET}/{path}")

# Função para acionar a AWS Lambda
def trigger_lambda():
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName=LAMBDA_FUNCTION_NAME,
        InvocationType='Event'
    )
    print("Lambda acionada para processar os dados!")

# AWS Lambda Handler para iniciar Glue Job
def lambda_handler(event, context):
    glue = boto3.client('glue')
    response = glue.start_job_run(JobName=GLUE_JOB_NAME)
    print("Glue Job iniciado com sucesso!")
    return {
        'statusCode': 200,
        'body': json.dumps('Glue Job Iniciado!')
    }

# Executando as funções
df = scrape_b3()
save_to_s3(df, RAW_PATH)
trigger_lambda()
