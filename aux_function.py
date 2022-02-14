import pandas as pd
import numpy as np
import io
from fuzzywuzzy import fuzz
import boto3 as b3
from boto.s3.connection import S3Connection
from io import StringIO
import config
import requests


#--------------- LEITURA DE ARQUIVOS -------------------------------------------------
def load_s3(KEY_S3,TYPE_DOC,ENC_T):

    sessao=b3.Session(aws_access_key_id=config.AWS_S3_ACCESS,
              aws_secret_access_key=config.AWS_S3_SECRET,
              region_name='sa-east-1')

    cliente=sessao.client('s3')
    g_obj = cliente.get_object(Bucket=config.BUCKET_QUALI, Key=KEY_S3)
    if TYPE_DOC == 0:
        body = g_obj['Body']
        g_obj = body.read().decode(encoding=ENC_T)
    else:
        g_obj = g_obj['Body'].read()
    return g_obj

def load_base_treino_s3(key_s3, enc_t, delimitador):
    sessao=b3.Session(aws_access_key_id=config.AWS_S3_ACCESS_TREINO,
              aws_secret_access_key=config.AWS_S3_SECRET_TREINO,
              region_name='sa-east-1')
    #Qual serviço eu desejo acessar com minha conexão
    cliente=sessao.client('s3')
    #base unificada
    BUCKET_S3=config.BUCKET_QUALI_TREINO
    #csv_obj = cliente.get_object(Bucket=BUCKET_S3, Key='D1_BASE/LEADS/STG_BASE_VENDAS_UNIFICADA.csv')
    csv_obj = cliente.get_object(Bucket=BUCKET_S3, Key=key_s3)
    body = csv_obj['Body']
    csv_string = body.read().decode(encoding=enc_t)
    
    df_leads = pd.read_csv(StringIO(csv_string), error_bad_lines=False, delimiter=delimitador)
    return df_leads 

#--------------- LEITURA DE MODELOS ---------------------------------------

def load_model_s3(KEY_S3):

    sessao=b3.Session(aws_access_key_id = config.AWS_S3_ACCESS,
              aws_secret_access_key = config.AWS_S3_SECRET,
              region_name='sa-east-1')

    cliente=sessao.client('s3')
    g_obj = cliente.get_object(Bucket='motorleads', Key=KEY_S3)
    g_obj = g_obj['Body'].read()
    
    return g_obj

def upload_file(path, bucket, bucket_path):
    s3 = b3.resource(
        's3', aws_access_key_id=config.AWS_S3_ACCESS,
        aws_secret_access_key=config.AWS_S3_SECRET)
    s3.meta.client.upload_file(path, bucket, bucket_path)

#--------------- CUSTOM GLOB ------------------------------------------------
def c_glob(sea):
    conn = S3Connection(config.AWS_S3_ACCESS, config.AWS_S3_SECRET)
    bucket = conn.get_bucket('motorleads')
    f_list = []
    cf_list = []
    for key in bucket.list():
        f_list.append(key.name)
    for fi in f_list:
        if sea in fi:
            cf_list.append(fi)
    return cf_list
#--------------- SALVAR CSV --------------------------------------------------------
def sav_csv(dframe,csv_pathname):
    bucket = config.BUCKET_QUALI
    csv_buffer = StringIO()
    dframe.to_csv(csv_buffer)
    s3_resource = b3.resource('s3', aws_access_key_id = config.AWS_S3_ACCESS, aws_secret_access_key = config.AWS_S3_SECRET)
    s3_resource.Object(bucket, csv_pathname).put(Body=csv_buffer.getvalue())


def diferenca_tempo(x1, x2, formato):
    #retorna a diferença de tempo entre duas datas
        return pd.to_datetime(x1, format=formato)-pd.to_datetime(x2, format=formato)
    
def Age_bins(coluna_idade):
    #faz a partição das datas
    return pd.cut(x=coluna_idade, bins=[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100], 
                        labels=['1-10', '11-20', '21-30', '31-40', '41-50', '51-60', 
                                '61-70', '71-80', '81-90', '91-100']).astype('O') 
def Score_bins(coluna_idade):
    #faz a partição do score
    return pd.cut(x=coluna_idade, bins=[0, 0.2, 0.4, 0.6, 0.8, 1], 
                        labels=['Muito Baixo', 'Baixo', 'Médio', 'Alto', 'Muito Alto']  ).astype('str')

def randargmax(b,**kw):
    """ No argmax comum é em caso de empate é selcionado o primeiro valor. Ex: argmax([1,2,2]) = 1.
    O randargmax sleeciona aleatoriamente um indice dos valores: Ex: randargmax([1,2,2])= 1 ou 2."""
    return np.argmax(np.random.random(b.shape) * (b==b.max()), **kw)


def print_to_string(*args, **kwargs):
    output = io.StringIO()
    print(*args, file=output, **kwargs)
    contents = output.getvalue()
    output.close()
    return contents

def limpeza_whitespace(df):
    #faz a o trim dos dados
    for i in range(0,df.shape[1]):
        if df.dtypes[i]=='O':
            df[df.columns[i]]=df[df.columns[i]].astype(str).str.lstrip().str.rstrip()
    return(df)

def dict_similar_f(coluna_a_tratar, coluna_padrao, similaridade):
    #compara e corrige dados de uma coluna com outra com os dados corrigidos
    dict_similar = {}
    lista_diferenca = list(set(coluna_a_tratar)-set(coluna_padrao))
    for j in range(len(lista_diferenca)):
        f = []
        for i in coluna_padrao:
            f.append(fuzz.ratio(lista_diferenca[j], i))
        if np.max(f)>similaridade:
            dict_similar[lista_diferenca[j]] = coluna_padrao[np.argmax(f)]
    return dict_similar

def limiar_param(corr_interno, corr_plantao, fixo=False, limiar_fixo = 0.03, plantao=config.PLANTAO):
    if not fixo:
        if plantao:
            try:
                limiar = 1/len(corr_plantao)
            except ZeroDivisionError:
                limiar = limiar_fixo
        else:
            try:
                limiar =  1/len(corr_interno)
            except ZeroDivisionError:
                limiar = limiar_fixo
    else:
        limiar = limiar_fixo
    requests.request("POST", config.URL_BEFAIR, headers=config.HEADERS, data={'valor':limiar})
    return limiar
        
