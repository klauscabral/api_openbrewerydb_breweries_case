#################################################################################################
###<desenvolvedor>: klaus cabral                                                           
###<data_criacao>: 12/09/2024
###<desc>: 
#################################################################################################
import sys
import os
import pandas as pd
import boto3  
from awsglue.utils import getResolvedOptions
import time
import awswrangler as wr   


# Inicializa os clientes
glue_client = boto3.client('glue', region_name='us-east-1')
sns_client = boto3.client('sns')

bronze_database='bronze'
silver_database='silver'
gold_database='gold'
table='tb_api_openbrewerydb_breweries'


jobname_bronze='jb_ing_api_openbrewerydb_breweries_to_bronze'
jobname_silver='jb_ing_api_openbrewerydb_breweries_to_silver'
jobname_gold='jb_gold_openbrewerydb_breweries'

# Configurações do SNS
sns_topic_arn = 'arn:aws:sns:us-east-1:373249867868:send_api_openbrewerydb_breweries_pipeline_status'



########################
## Functions   
########################


def send_notification(message):
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=message,
        Subject='Notificação de Processamento do Glue'
    )

## Function responsible for checking the existence of the table in the environment.
def check_table_existence_in_catalog(database_name, table_name):
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



def run_job_and_control_execution(jobname):
    glue_client = boto3.client('glue')
    try:
        print('###############################################################################')
        print(f'Iniciando processamento do JOB --> {jobname}')
        print('###############################################################################')

        print(f"==== Inicio Chamada ========")
        exec_job        =jobname
        response        = glue_client.start_job_run(JobName=exec_job)
        status          = glue_client.get_job_run(JobName=exec_job, RunId=response['JobRunId'])
        state           = status['JobRun']['JobRunState']
        id              = status['JobRun']['Id']
        Execution_time  = status['JobRun']['ExecutionTime']

        jobs=[]
        jobs.append({f'exec_job': exec_job,f'response': response,f'status': status,f'state': state,f'id': id,f'Execution_time': Execution_time})

        while (jobs[0]['state'] not in ['SUCCEEDED'] )  :
            print("Status do job  {0} no ID de execucao {1}:    {2}".format(jobs[0][f'exec_job'],jobs[0][f'id'],jobs[0][f'state']), end = '\n')
            print("Aguardando 10 Segundos ..")
            time.sleep(10)
    
            # pega novo status do job.
 
            jobs[0]['state'] = glue_client.get_job_run(JobName=jobs[0]['exec_job'], RunId=jobs[0]['response']['JobRunId'])['JobRun']['JobRunState']
            jobs[0]['Execution_time'] = glue_client.get_job_run(JobName=jobs[0]['exec_job'], RunId=jobs[0]['response']['JobRunId'])['JobRun']['ExecutionTime']

            if (jobs[0]['status'] in ['STOPPED', 'FAILED', 'TIMEOUT', 'STOPPING'] ):
                print(f"Jobs  finalizados por questao de falha, verifique o log das instancias do jobs  ==> {jobname} \n")
                raise Exception('Failed to execute glue job: ')
            
        print("JOb 			==> {0} \nFinalizando com status 	==> {1}!! \nTempo de Execução total ==> {2}".format(jobname,jobs[0][f'state'],jobs[0][f'Execution_time']), end = '\n')
        print("==== Fim da chamada=====")
        return True
    except:
        raise Exception(f'Failed to execute glue job: {jobname} ')
        return False

########################################################################
## Iniciando Pipeline:
########################################################################

check_jb_ing_api_openbrewerydb_breweries_to_bronze=run_job_and_control_execution(jobname_bronze)
check_jb_ing_api_openbrewerydb_breweries_to_silver=run_job_and_control_execution(jobname_silver)
Check_jb_gold_openbrewerydb_breweries=run_job_and_control_execution(jobname_gold)

########################################################################
## Iniciando verificações de qualidade  :
########################################################################

# verificando se as tabela existem em seus respectivas camadas:
check_table_exists_In_bronze=check_table_existence_in_catalog(bronze_database, table)
check_table_exists_In_Silver=check_table_existence_in_catalog(silver_database, table)
check_table_exists_In_Gold=check_table_existence_in_catalog(gold_database, table)


## verificação de integridade Adicionais, verificando quantiidade de registros inseridos hoje na camada Bronze.

df = wr.athena.read_sql_query(sql=f""" select count(1) as qtd_registros from {bronze_database}.{table} where  anomesdia=date_format(current_date, '%Y%m%d') """,database=bronze_database,workgroup='primary')
qtd_reg_bronze = int(df.iloc[0, 0])

df = wr.athena.read_sql_query(sql=f""" select count(1) as qtd_registros from {silver_database}.{table} where  anomesdia=date_format(current_date, '%Y%m%d') """,database=silver_database,workgroup='primary')
qtd_reg_silver = int(df.iloc[0, 0])

df = wr.athena.read_sql_query(sql=f""" select count(1) as qtd_registros from {gold_database}.{table} where  anomesdia=date_format(current_date, '%Y%m%d') """,database=gold_database,workgroup='primary')
qtd_reg_gold = int(df.iloc[0, 0])


stmt='################################################################################# \n'    
stmt = stmt + ' ========== PIPELINE FINALIZADO  ==========  \n'   
stmt = stmt + '#################################################################################\n'  
stmt = stmt + f'JOB Executado com sucesso na camada BRONZE --> {check_jb_ing_api_openbrewerydb_breweries_to_bronze}\n'   
stmt = stmt + f'JOB Executado com sucesso na camada SILVER --> {check_jb_ing_api_openbrewerydb_breweries_to_silver}\n'   
stmt = stmt + f'JOB Executado com sucesso na camada GOLD --> {Check_jb_gold_openbrewerydb_breweries}\n'   
stmt = stmt + f'Tabela Criada com sucesso na camada BRONZE --> {check_table_exists_In_bronze}\n'  
stmt = stmt + f'Tabela Criada com sucesso na camada SILVER --> {check_table_exists_In_Silver}\n'  
stmt = stmt + f'Tabela Criada com sucesso na camada GOLD --> {check_table_exists_In_Gold}\n'  
stmt = stmt + f'Quantidade de Linhas na camada BRONZE (hoje) -->  {qtd_reg_bronze}\n'  
stmt = stmt + f'Quantidade de Linhas na camada SILVER (hoje)-->  {qtd_reg_silver}\n'  
stmt = stmt + f'Quantidade de Linhas na camada GOLD (Agg)-->  {qtd_reg_gold}\n'  

send_notification(stmt)

sys.stdout.flush()
print('Fim do processamento do Pipeline')










