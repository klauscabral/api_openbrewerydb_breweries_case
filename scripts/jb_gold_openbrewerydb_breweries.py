#################################################################################################
###<desenvolvedor>: klaus cabral                                                           
###<data_criacao>: 12/09/2024
###<desc>: 
#################################################################################################

import sys
import boto3
import requests
import json
import pandas as pd
from datetime import *
import awswrangler as wr

#Configuracao
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 3000)

# Configurações do banco de dados e tabelas
source_database_name = 'silver'
target_database_name = 'gold'
source_table_name = 'tb_api_openbrewerydb_breweries'
target_table_name = 'tb_api_openbrewerydb_breweries'


########################
## Functions   
########################

## Função responsavel por checar a existencia da tabela no ambiente:
def check_table_existence(database_name, table_name):
    glue_client = boto3.client('glue')
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        if 'Table' in response:
            return True  # The table exists in the Glue catalog
        else:
            return False  # The table does not exist in the Glue catalog
    except:
        return False
        pass


# Lê a tabela origem do Athena usando uma consulta SQL  
# aqui vc pode usar a logica de acordo com a origem dos dados, ou pegar somente a ultima informacao ou fazer o merge / update de todos os dados, vai depender de como funciona a atualização dos dados na origem
# para esse estudo isso nao foi considerado por que nao temos detalhes de como funcina a fonte de dados, utilizamos um filtro para pegar somente a "ultima data de processamento" somente para manter integridade dos processo visto que os dados na bronze irão se duplicar ao longo do tempo caso o retorno da API seja sempre o mesmo.

query = f"""select  country, state_province, brewery_type, count(1) as qtd_of_breweries, anomesdia
 from  {source_database_name}.{source_table_name} group by country, state_province, brewery_type , anomesdia order by 4 desc"""
df = wr.athena.read_sql_query(sql=query, database=source_database_name)


print(df)

print(" Checking if the Statements table exists in the Environment ")
check_breweries_table_exists = check_table_existence( target_database_name, target_table_name)

if check_breweries_table_exists:
    print(f"table {target_table_name} exists in the {target_database_name} layer")
    #clean table in Silver layer.
    print("Cleaning table in the clean layer")
    wr.athena.start_query_execution(sql=f""" delete from {target_database_name}.{target_table_name} """,  database=target_database_name,workgroup="primary",wait=True )
    print("Optimizing table in the clean layer")
    wr.athena.start_query_execution(sql=f""" OPTIMIZE {target_database_name}.{target_table_name} REWRITE DATA USING BIN_PACK """,  database=target_database_name,workgroup="primary",wait=True )
    print("running VACCUM table in the clean layer")
    wr.athena.start_query_execution(sql=f""" vacuum  {target_database_name}.{target_table_name} """,  database=target_database_name,workgroup="primary",wait=True )
    print("Dropping table into clean layer")
    wr.athena.start_query_execution(sql=f""" drop table  {target_database_name}.{target_table_name} """,  database=target_database_name,workgroup="primary",wait=True )
else:
    print(f"table {target_table_name} DOES NOT exists in the {target_database_name} layer")

# Escreve o DataFrame no formato Iceberg
wr.athena.to_iceberg(
    df=df,
    database=target_database_name,
    table=target_table_name,
    temp_path='s3://373249867868-gold-layer/data/temp',
    keep_files =False,
    table_location='s3://373249867868-gold-layer/data/api/openbrewerydb/breweries/',
    partition_cols=['state_province'],
    workgroup='primary'
)

print(f'Tabela {target_table_name} criada com sucesso no formato Iceberg.')
print('FIm do JOB')
