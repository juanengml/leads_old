import glob
import pandas as pd
import config
from datetime import datetime
import os
import pymsteams
import aux_function
import joblib

def dados_recebidos(historico_recebidos):
    historico_recebidos.drop_duplicates(subset=['data', 'ID_LEAD'], inplace=True)
    
    all_files = glob.glob(config.path_recebidos + "/[0-9]*"+".csv")
    
    li = []
    
    for filename in all_files:
    
        try:
            df = pd.read_csv(filename, index_col=None, header=0, delimiter = ",")
            df["data"]=datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%Y%m%d')
            li.append(df)
            
        except: pass
    
    try:
        recebidos_1 = pd.concat(li, axis=0, ignore_index=True)
        recebidos = recebidos_1.drop_duplicates(subset=['data', 'ID_LEAD'])
    except: pass
    
    dados_empilhados_recebidos = historico_recebidos.append(recebidos).drop_duplicates(subset=['data', 'ID_LEAD'])
    
    dados_empilhados_recebidos = dados_empilhados_recebidos.drop_duplicates(subset=['data', 'ID_LEAD']).reset_index().drop('index', axis=1)
    
    dados_empilhados_recebidos['data'] = dados_empilhados_recebidos['data'].astype(str).astype(int)
    dados_empilhados_recebidos.drop_duplicates(subset=['data', 'ID_LEAD'], inplace=True)
    return dados_empilhados_recebidos

def dados_enviados(historico_enviados):
    historico_enviados.drop_duplicates(subset=['data', 'ID_LEAD'], inplace=True)
    
    all_files = glob.glob(config.path_enviados + "/*"+".csv")
    
    li = []
    
    for filename in all_files:
    
        try:
            df = pd.read_csv(filename, index_col=None, header=0, delimiter = ";")
            df["data"]=datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%Y%m%d')
            li.append(df)
            
        except: pass
        
    try:     
        enviados = pd.concat(li, axis=0, ignore_index=True)
        enviados.drop_duplicates(subset=['data', 'ID_LEAD'], inplace=True)
        #lista de corretores que já trabalharam hoje por id do lead
        enviados_2 = enviados
        enviados_2['ID_LEAD'] = enviados_2[',ID_LEAD'].apply(lambda x: x.split(',')[1]).astype(float)
    except: pass


            
    dados_empilhados_enviados = historico_enviados.append(enviados).drop_duplicates(subset=['data', 'ID_LEAD'])
    
    dados_empilhados_enviados = dados_empilhados_enviados.drop_duplicates(subset=['data', 'ID_LEAD']).reset_index().drop('index', axis=1)
    
    dados_empilhados_enviados=dados_empilhados_enviados.drop_duplicates(subset=['data', 'ID_LEAD'])
    
    dados_empilhados_enviados['data'] = dados_empilhados_enviados['data'].astype(str).astype(int)
    dados_empilhados_enviados.drop_duplicates(subset=['data', 'ID_LEAD'], inplace=True)
    
    dados_empilhados_enviados = dados_empilhados_enviados[dados_empilhados_enviados["ID_LEAD"]>0]
    
    var = ['ID_LEAD','CPF_CORRETOR','CPF_CORRETOR_2','CPF_CORRETOR_3','RATING','RATING_2','RATING_3','SCORE','data']
    dados_empilhados_enviados = dados_empilhados_enviados[var]
    return dados_empilhados_enviados


def salva_e_limpa_pasta_de_controle(pasta_save, pasta_historico):

    all_files = glob.glob(pasta_save + "/[0-9]*"+".csv")

    try:
        for filename in all_files:
                arq = pd.read_csv(filename)
                arq_name = filename[len(pasta_save)+1:]  
                arq.to_csv(pasta_historico+'/backup/'+arq_name)
        
                os.remove(filename)
    except: pass
        
    
def Teams_message(URL_Teams, recomendacao, tipo_score=False):
    if tipo_score:
            texto = aux_function.print_to_string(
                "\n" , len(recomendacao), "leads recebidos e classificados:"
                "\n", len(recomendacao[recomendacao['segmentacao']=='Muito Baixo']), 'leads com score Muito Baixo',
                "\n", len(recomendacao[recomendacao['segmentacao']=='Baixo']), 'leads com score Baixo',
                "\n", len(recomendacao[recomendacao['segmentacao']=='Médio']), 'leads com score Médio',
                "\n", len(recomendacao[recomendacao['segmentacao']=='Altoi']), 'leads com score Alto',
                "\n", len(recomendacao[recomendacao['segmentacao']=='Muito Alto']), 'leads com score Muito Alto')
    else:    
        if config.PLANTAO:
            texto = aux_function.print_to_string("Plantão ativado."
                "\n" , len(recomendacao), "leads recebidos e distribuidos")
        else:
            texto = aux_function.print_to_string(
                "\n" , len(recomendacao), "leads recebidos e distribuidos")
    
    myTeamsMessage = pymsteams.connectorcard(URL_Teams)
    
    myTeamsMessage.title("Motor foi acionado♥")
    myTeamsMessage.color("#00ff00")
    myTeamsMessage.text(texto)
    myTeamsMessage.send()