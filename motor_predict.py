#-----------------------------------------------------------------------------
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
from artefatos import categoria_treino, base_fair_motor, kmeans_model
from artefatos import enviados_count_corretor_redistribuicao, features, features_cluster

    
import random 
#k = str(random.randint(10000, 100000000))
#tste_csv = pd.read_csv('teste_base.csv')
#teste_sample = tste_csv.sample(10)
#tste_csv.to_json('teste_sample.json')

#df = teste_sample.drop(['MIDIA_VEICULO'], axis=1)
def motor (pay):
    #--------------- LIBRARIES ------------------------------------------------------------------------------

    #--------------- CORRETORES ATIVOS API ------------------------------------------------------------------------------
    # print("corretore inicio"+str(datetime.now()))
    corretores_ativos, corr_interno, corretores_disponiveis = corretores_lib.corr_ativos()
    # print("corretore fim"+str(datetime.now()))
    # ---------------- CAPACITY CORRETORES ----------------------------------------------------------------------------
    
    #capacity_restante = corretores_lib.capacity_restante(capacity_corretor)
    #corretores_limiar_capacity = list(capacity_restante[capacity_restante.capacity_restante>0].CPF_CORRETOR)
    if config.PLANTAO:
        filtro_plantao = corretores_lib.filtro_plantao()
    else:
        filtro_plantao=[]
    # print("corretore plantão"+str(datetime.now()))
    #--------------- PARAMETROS DA DISTRIBUIÇÃO ------------------------------------------------------------------------
    limiar_param = aux_function.limiar_param(corr_interno, filtro_plantao, fixo=False, limiar_fixo = 0.04, plantao=config.PLANTAO)

    grau_justica = 0.1


      
    #-------------------- CONTROLE DE DATA --------------------------------------------------------    
    
    ano = datetime.today().year
    mes = datetime.today().month
    dia = datetime.today().day
    data = str(ano)+str(mes)+str(dia)
    today = str(datetime.today())[:10]
    #anomes = str(ano)+str(mes)
    

    date_today = dt.date.today()
    date_inicial = date_today + dt.timedelta(days=-30)

    xvar = features['xvar']
    cat = features['cat']
    num = features['num']
   
    #------------------ DADOS DE INPUTS-------------------------------------------------------------------

    df = pd.DataFrame(data=pay)
    
    if 'FL_CLIENTE_ATIVO' not in df.columns:
        df['FL_CLIENTE_ATIVO'] = False
    
    #-------------------- SALVA LOG --------------------------------------------------------
    logging.basicConfig(filename=config.path_2+'/'+'log_motor_'+data+'.log', filemode='a', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.warning(len(df))
        
    #-------------------- PREPARAÇÃO DE DADOS DO INPUT --------------------------------------------------------
    #caso a coluna de treinamento não esteja na base, criar uma coluna com dados nan.
    print("Prepara dados inicio"+str(datetime.now()))
    for col in ['MIDIA_VEICULO', 'MIDIA_FORMATO', 'PROFISSAO', 'ENTIDADE', 'MUNICIPIO', 'GRUPO_ORIGEM']:
        if col not in df.columns:
            df[col] = np.nan 
    df['ORIGEM_DE_MIDIA_VEICULO'] = df['MIDIA_VEICULO'].astype(str)
    df['ORIGEM_DE_MIDIA_FORMATO'] = df['MIDIA_FORMATO'].astype(str)
    df['ds_profissao'] = df['PROFISSAO'].astype(str)
    df['EntidadeLead'] = df['ENTIDADE'].astype(str)
    df['CIDADE'] = df.MUNICIPIO.astype(str).apply(lambda x: unidecode.unidecode(x.lower()))
    df['GRUPOORIGEM'] = df.GRUPO_ORIGEM.astype(str).apply(lambda x: unidecode.unidecode(x.lower()))
    df['NM_ORIGEM'] = df.apply(lambda df: df.GRUPOORIGEM if (df.NM_ORIGEM=='nan' or df.NM_ORIGEM==np.nan) else df.NM_ORIGEM, axis=1)


    df_f = prepara_dados.data_prep(df)

    # print("Prepara dados fim"+str(datetime.now()))
    #-------------------- LOOP DA DISTRIBUIÇÃO --------------------------------------------------------
    #coontém as variáveis necessárias para predição
    leads_recomendacao = []
    for i in (range(len(df_f))):
        df_1 = pd.DataFrame(df_f.loc[i,:]).T
        try:
            origens_novas, df_le = prepara_dados.data_clean(df_1, cat, num)
            if origens_novas['NM_ORIGEM'] !=set():
                print('Nova origem não identificada:', origens_novas['NM_ORIGEM'])
                base_teste_ = base_teste.append(pd.DataFrame(df_1))
                lista_novas_origens.append(list(origens_novas['NM_ORIGEM'])[0])
                origens_novas = set()
            #-----------------------------preprocessamento e predição (Proprensão do score de leads)-----------------------------
            df_le_2 = preprocessor.transform(df_le[xvar])
            df_le_2 = pd.DataFrame(df_le_2)
            df_le_2.columns = num+cat
            predito = xgboost_loaded_model.predict_proba(df_le_2[xvar].values)
            df_1['score'] = predito[:,1]
            df_1['flag_previsao'] = xgboost_loaded_model.predict(df_le_2[xvar].values)
            df_1['Score_xgboost_bin'] = aux_function.Score_bins(df_1['score'])
            df_1['age_bins'] = aux_function.Age_bins(df_1['DATA_NASCIMENTO'])
            #Variáveis para CLusterização
            bases = prepara_dados.data_cluster_prep(df_1, features_cluster)
        
            # In[4]:       
            #Feito a recomendação, o modelo deve recebe uma matriz M(C,L), 
            #onde C é o corretor e L o lead distribuído. Cada elemento dessa matriz será uma pontuação dada pelo corretor ao cluster.
            #Apenas corretores com certo histórico podem ser recomendados de maneira cognitiva
            # --------------------------------------------------------------------------------------------
            # LIMITAÇÃO DE LEADS DIARIO
            # print("corretor fair "+str(datetime.now()))
            corr_full, corretor_limiar = corretores_lib.fair_motor(base_fair_motor, limiar_param)
            # ---------------------------------------------------------------------------------------
            corr_ativos = list(corretores_ativos['cpf'].unique())
            # print("corretor fair fim "+str(datetime.now()))
            matriz_final, corretores_filtrados, redistribuido = corretores_lib.corr_filtros(matriz, corretores_disponiveis, corr_ativos, filtro_plantao,
                                                                                            corretor_limiar, bases, grau_justica, plantao=config.PLANTAO)
            # ----------------------------------------------------------------------------------------------
            #corretores = corretores[corretores.Vendas>=5].sort_values(by='corretor_id')
            # print("corretor filtro fim "+str(datetime.now()))

        
            # In[6]:
            # capacity diário dos corretores e leads distribuidos anteriormente no mesmo dia para cada corretor
            # essa tabela contém quantos leads cada corretor pode receber diariamente 
            # e quantos leads cada corretor ja recebeu nas distribuições das requisoções anteriores do mesmo dia
            # obs: se for a primeira requisição do dia a coluna leads_distribuidos_anteriormente devem ser todas zeros
            # após a distribuição de leads a coluna leads_distribuidos_anteriormente deve ser atualizada com a quantidade 
            # de leads que foram distribuidos e somadas nas iterações futuras do mesmo dia
            recomendacao = corretores_lib.recommender(matriz_final, bases, redistribuido)
            if len(recomendacao)==0:
                raise ValueError
            recomendacao = recomendacao.replace(np.nan, pd.DataFrame(corretores_ativos['cpf'].sample(1)).iat[0,0])
            
            leads_recomendacao.append(recomendacao)
            
            print("lead",df_1['ID_LEAD'].iloc[0],"distribuição otimizada")
        
            
        except (IndexError, ValueError):
            # LIMITAÇÃO DE LEADS DIARIO
            
            corr_full, corretor_limiar = corretores_lib.fair_motor(base_fair_motor, limiar_param)
            corretores_ativos2 = corretores_ativos
            #print(len(corretores_ativos))
            corretores_ativos2 = corretores_ativos2[corretores_ativos2['cpf'].isin(corr_interno)]
            corretores_ativos2 = corretores_ativos2[~corretores_ativos2['cpf'].isin(corretor_limiar)]

            try:
                r = enviados_count_corretor_redistribuicao[enviados_count_corretor_redistribuicao.data_referencia==today].groupby('ID_LEAD')['CPF_CORRETOR'].apply(list)
                if bases['ID_LEAD'][0] in list(r.index):
                    redistribuido = True
                    corretores_ativos2 = corretores_ativos2[~corretores_ativos2['cpf'].isin(r[bases['ID_LEAD'][0]])]
                else:
                    redistribuido = False
            except:
                redistribuido = False
        
            recomendacao = corretores_lib.random_recommender(corretores_ativos2, df_1, config.PLANTAO, filtro_plantao)
            recomendacao['FLAG_REDISTRIBUICAO'] = redistribuido
            leads_recomendacao.append(recomendacao)
            print("lead",df_1['ID_LEAD'].iloc[0],"distribuição randômica")
    
        # COLOCAR ISSO NO FINAL DO BLOCO DE LOOP (MAS AINDA DENTRO DO LOOP)
        #print(len(leads_recomendacao))
        # print("inicio concat leads recomendação "+str(datetime.now()))
        leads_recomendacao_pd = pd.concat([item for item in leads_recomendacao])
        leads_recomendacao_pd.to_csv(config.path_enviados+'/'+'recomendacao_'+str(ano)+str(mes)+str(dia)+'.csv', sep=';', index=None)

        
        # print("inicio dados recebidos "+str(datetime.now()))
        #---------------------------------dados dos recebidos -> empilhar no histórico recebidos (tudo)----------------
        from artefatos import historico_recebidos
        dados_empilhados_recebidos = historico.dados_recebidos(historico_recebidos)
        
        # print("inicio dados enviados "+str(datetime.now()))
        #--------------------dados dos enviados -> empilhar no histórico enviados (tudo)-------------------------------   
        from artefatos import historico_enviados
        dados_empilhados_enviados = historico.dados_enviados(historico_enviados)
    
        # print("inicio dados enviados  30 dias "+str(datetime.now()))    
        #----------------------------------dados dos enviados -> últimos 30 dias do histórico de enviados--------------------------  
        
        dados_empilhados_motor = dados_empilhados_enviados[dados_empilhados_enviados["data"]>int(date_inicial.strftime('%Y%m%d'))]
        dados_empilhados_motor = dados_empilhados_motor[dados_empilhados_motor["ID_LEAD"]>0]
        
        # print("inicio grava recebido e enviados "+str(datetime.now()))
        # --------------------------------------------------------------------------------------------------------------------------
        dados_empilhados_recebidos.to_csv(config.path_recebidos_historico+'/historico_recebidos.csv', index=False)
        dados_empilhados_enviados.to_csv(config.path_enviados_historico +'/historico_enviados.csv', index=False)
        dados_empilhados_motor.to_csv(config.path_fair_motor+'/dados_empilhados_motor.csv', index=False)
    recomendacao = pd.concat([item for item in leads_recomendacao])
    # print("for variaveis predição "+str(datetime.now()))

    # In[14]:
    
      
    #-------------------------------------------------------------------------------------------    
    env = recomendacao[['ID_LEAD', 'CPF_CORRETOR']]
    env['data_referencia'] = today
    env_re = enviados_count_corretor_redistribuicao.append(env, ignore_index=True)[['ID_LEAD', 'CPF_CORRETOR', 'data_referencia']]
    env_re = env_re[env_re.data_referencia == today]
    env_re.to_csv(config.path_3+'/'+'enviados_count_corretor_redistribuicao.csv')
    recomendacao.to_csv(config.path_recomendacao +'/recomendacao_'+str(ano)+str(mes)+str(dia)+'.csv', sep=';', index=None)
    nome_exp = 'recomendacao_'+str(ano)+str(mes)+str(dia)+'.csv'
    #------------------------ salva arquivos no histórico e limpa pasta de controle -------------------
        
    historico.salva_e_limpa_pasta_de_controle(config.path_recebidos, config.path_recebidos)
    historico.salva_e_limpa_pasta_de_controle(config.path_enviados+'/enviados_motor', config.path_enviados)
      
    # ----------------- aviso do bot no teams que o motor rodou
    try:
        historico.Teams_message(config.URL_TEAMS, recomendacao)
    except: pass
    
        #aciona o script de retreino
    joblib.dump(lista_novas_origens, config.path_3+'/'+'lista_novas_origens.joblib.dat')
    #try:
    #    base_teste_.to_csv(config.path_3+'/'+'base_teste.csv')
    #except: pass
    
    return nome_exp