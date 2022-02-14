from fuzzywuzzy import fuzz
import logging
import pandas as pd
import glob
import warnings
warnings.filterwarnings("ignore")
import numpy as np
import joblib
import unidecode
import json
import regex as re
from datetime import datetime
import os
import urllib3
urllib3.disable_warnings()
import config
import corretores_lib
import datetime as dt
import aux_function
import historico
import prepara_dados
from artefatos import capacity_corretor, profissao_lista_representativa, base_teste
from artefatos import MIDIA_VEICULO_lista_representativa, Listas, xgboost_loaded_model, preprocessor, preprocessor_cluster
from artefatos import lista_novas_origens, ddd, idhm, matriz, corretores
from artefatos import categoria_treino, base_fair_motor, kmeans_model, df_calibracao_score
from artefatos import enviados_count_corretor_redistribuicao, features, features_cluster
import app
#k = str(random.randint(10000, 100000000))
#teste_csv = pd.read_csv('teste_base.csv')
#teste_sample = teste_csv.sample(100)
#teste_sample.to_json('teste_sample.json')

def motor (pay):
    ano = datetime.today().year
    mes = datetime.today().month
    dia = datetime.today().day
    data = str(ano)+str(mes)+str(dia)
    #anomes = str(ano)+str(mes)
    

    date_today = dt.date.today()
    date_inicial = date_today + dt.timedelta(days=-30)

    xvar = features['xvar']
    cat = features['cat']
    num = features['num']
   
    #------------------ DADOS DE INPUTS-------------------------------------------------------------------
    
    df = pd.DataFrame(data=pay)
    #df= teste_csv
    #df = pd.DataFrame({})
        
    #df.to_csv(config.path_recebidos+'/'+k+'_recebido_'+data+'.csv', index=None)
    #df = pd.DataFrame(data=teste_csv)
    #df = df.sample(100)    
    if 'FL_CLIENTE_ATIVO' not in df.columns:
        df['FL_CLIENTE_ATIVO'] = False
    
    #-------------------- SALVA LOG --------------------------------------------------------
    logging.basicConfig(filename=config.path_2+'/'+'log_motor_'+data+'.log', filemode='a', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.warning(len(df))
        
    #-------------------- PREPARAÇÃO DE DADOS DO INPUT --------------------------------------------------------
    #caso a coluna de treinamento não esteja na base, criar uma coluna com dados nan.
    for col in ['MIDIA_VEICULO', 'MIDIA_FORMATO', 'PROFISSAO', 'ENTIDADE', 'MUNICIPIO', 'GRUPO_ORIGEM']:
        if col not in df.columns:
            df[col] = np.nan 
    df['ORIGEM_DE_MIDIA_VEICULO'] = df['MIDIA_VEICULO'].astype(str)
    df['ORIGEM_DE_MIDIA_FORMATO'] = df['MIDIA_FORMATO'].astype(str)
    df['ds_profissao'] = df['PROFISSAO'].astype(str)
    df['EntidadeLead'] = df['ENTIDADE'].astype(str)
    df['CIDADE'] = df.MUNICIPIO.astype(str).apply(lambda x: unidecode.unidecode(x.lower()))
    df['GRUPOORIGEM'] = df.GRUPO_ORIGEM.astype(str).apply(lambda x: unidecode.unidecode(x.lower()))

    df_f = prepara_dados.data_prep(df)

 
    #-------------------- LOOP DA DISTRIBUIÇÃO --------------------------------------------------------
    #coontém as variáveis necessárias para predição
    leads_recomendacao = []
    for i in (range(len(df_f))):
        #df_1 = pd.DataFrame(df_f[df_f.ID_LEAD==4825430.0].loc[131,:]).T
        df_1 = pd.DataFrame(df_f.loc[i,:]).T
        origens_novas, df_le = prepara_dados.data_clean(df_1, cat, num)
        if origens_novas['NM_ORIGEM'] !=set():
            base_teste_ = base_teste.append(pd.DataFrame(df_1))
            lista_novas_origens.append(list(origens_novas['NM_ORIGEM'])[0])
        #-----------------------------preprocessamento e predição (Proprensão do score de leads)-----------------------------
        df_le_2 = preprocessor.transform(df_le[xvar])
        df_le_2 = pd.DataFrame(df_le_2)
        df_le_2.columns = num+cat
        predito = xgboost_loaded_model.predict_proba(df_le_2[xvar].values)
        df_1['score'] = predito[:,1]
        df_1['flag_previsao'] = xgboost_loaded_model.predict(df_le_2[xvar].values)
        df_1['segmentacao'] = aux_function.Score_bins(df_1['score'])
        df_1['age_bins'] = aux_function.Age_bins(df_1['DATA_NASCIMENTO'])
        df_1['prob_conversao'] = df_calibracao_score.iloc[np.argmin(np.abs(df_calibracao_score['Score']-float(df_1['score']))),2]
        recomendacao = df_1[['ID_LEAD', 'score', 'prob_conversao', 'segmentacao']]
        leads_recomendacao.append(recomendacao)
        print("lead",df_1['ID_LEAD'].iloc[0],"distribuição do score e da probabilidade de cconversão")
        joblib.dump(lista_novas_origens, config.path_3+'/'+'lista_novas_origens.joblib.dat')
        try:
            base_teste_.to_csv(config.path_3+'/'+'base_teste.csv')
        except: pass   
    leads_recomendacao_pd = pd.concat([item for item in leads_recomendacao])
    leads_recomendacao_pd.to_csv(config.path_recomendacao+'/'+'Score_'+str(ano)+str(mes)+str(dia)+'.csv', sep=';', index=None)
    nome_exp = 'Score_'+str(ano)+str(mes)+str(dia)+'.csv'
    try:
        historico.Teams_message(config.URL_TEAMS, leads_recomendacao_pd, True)
    except: pass

    return nome_exp
        