#################################################################################################
###<desenvolvedor>: klaus cabral                                                           
###<data_criacao>: 12/09/2024
###<desc>: Processo para coleta das informacoes da API "https://api.openbrewerydb.org/breweries"
#################################################################################################

import sys
import boto3
import requests
import json
import pandas as pd
from datetime import *
import awswrangler as wr

# Clientes.
s3 = boto3.client('s3')

#Configuracao
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 3000)

bucket_name = '373249867868-bronze-layer'
s3_path = f's3://{bucket_name}/data/api/openbrewerydb/breweries/'


# fazendo solicitacao para API (sem autenticacao mesmo...)
api_url = "https://api.openbrewerydb.org/breweries"
response = requests.get(api_url)

#Verificando response.
#print(response)

# Verifica se a requisição foi bem-sucedida
if response.status_code == 200:
    data = response.json()
    # Converte os dados para JSON
    json_data = json.dumps(data)    
    df = pd.read_json(json_data)
else:
    print(f'verifique Chamada / retorno API --> response.status_code --> {response.status_code}')
    

# adicionando coluna de particionamento para garantir controle das informacoes diarias.
df['anomesdia'] = datetime.now().strftime('%Y%m%d')

## adjusting datatypes.
#df['longitude'] = df['longitude'].astype(float)

## muda colunas para string
column_types = {col: str('string') for col in df.columns}
column_types.popitem()
#print(column_types)

# Grava o DataFrame no S3 em formato Parquet, particionado pela coluna ANOMESDIA
print('Escrevendo dados no s3 para que sejam interpretados via athena')
wr.s3.to_parquet(
    df=df,
    path=s3_path,
    partition_cols=['anomesdia'],
    mode='overwrite_partitions',
    dataset=True,
    database='bronze',
    table='tb_api_openbrewerydb_breweries',
    dtype =column_types

)

#escrevendo tambem dados brutos no s3 

data_atual = datetime.now().strftime('%Y%m%d')
nome_arquivo = f'openbrewerydb.org_breweries_request_{data_atual}.json'

s3_path_for_raw_file = f's3://{bucket_name}/raw_data/api/openbrewerydb/breweries/'

# Enviando o arquivo JSON para o S3
s3.put_object(Bucket=bucket_name, Key=f'raw_data/api/openbrewerydb/breweries/{nome_arquivo}', Body=json_data)

print(f'Arquivo {nome_arquivo} criado com sucesso no bucket {s3_path_for_raw_file}.')

print('FIm do JOB')
