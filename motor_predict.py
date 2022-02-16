import os
import glob
import json
import urllib3
import logging
import warnings
import regex as re
import datetime as dt
from datetime import datetime

import pandas as pd
import numpy as np
import joblib
import unidecode
from fuzzywuzzy import fuzz

import config
import corretores_lib
import aux_function
import historico
import prepara_dados
from artefatos import capacity_corretor, profissao_lista_representativa, base_teste
from artefatos import MIDIA_VEICULO_lista_representativa, Listas, xgboost_loaded_model, preprocessor
from artefatos import lista_novas_origens, ddd, idhm, matriz, corretores
from artefatos import categoria_treino, base_fair_motor, kmeans_model
from artefatos import enviados_count_corretor_redistribuicao, features, features_cluster
from artefatos import historico_recebidos, historico_enviados

warnings.filterwarnings("ignore")
urllib3.disable_warnings()


def chamar_bot_reams(message):
    try:
        historico.Teams_message(config.URL_TEAMS, message)
    except:
        pass


def obter_corretores():
    corretores_ativos, corr_interno, corretores_disponiveis = corretores_lib.corr_ativos()

    if config.PLANTAO:
        filtro_plantao = corretores_lib.filtro_plantao()
    else:
        filtro_plantao = []

    return corretores_ativos, corr_interno, corretores_disponiveis, filtro_plantao


def corrigir_dados_leads(leads):
    for col in ['MIDIA_VEICULO', 'MIDIA_FORMATO', 'PROFISSAO', 'ENTIDADE', 'MUNICIPIO', 'GRUPO_ORIGEM']:
        if col not in leads.columns:
            leads[col] = np.nan
    leads['ORIGEM_DE_MIDIA_VEICULO'] = leads['MIDIA_VEICULO'].astype(str)
    leads['ORIGEM_DE_MIDIA_FORMATO'] = leads['MIDIA_FORMATO'].astype(str)
    leads['ds_profissao'] = leads['PROFISSAO'].astype(str)
    leads['EntidadeLead'] = leads['ENTIDADE'].astype(str)
    leads['CIDADE'] = leads.MUNICIPIO.astype(str).apply(lambda x: unidecode.unidecode(x.lower()))
    leads['GRUPOORIGEM'] = leads.GRUPO_ORIGEM.astype(str).apply(lambda x: unidecode.unidecode(x.lower()))
    leads['NM_ORIGEM'] = leads.apply(
        lambda df: df.GRUPOORIGEM if (df.NM_ORIGEM == 'nan' or df.NM_ORIGEM == np.nan) else df.NM_ORIGEM, axis=1)
    return leads


def estimar_propensao_venda(df, num, cat, xvar):
    origens_novas, df_le = prepara_dados.data_clean(df, cat, num)
    df_le_2 = preprocessor.transform(df_le[xvar])
    df_le_2 = pd.DataFrame(df_le_2)
    df_le_2.columns = num + cat
    y = xgboost_loaded_model.predict(df_le_2[xvar].values)
    y_proba = xgboost_loaded_model.predict_proba(df_le_2[xvar].values)
    return y, y_proba, origens_novas


def recomendar_corretor(corretores_disponiveis, corretores_ativos, filtro_plantao, corretor_limiar, bases,
                        grau_justica):
    matriz_final, corretores_filtrados, redistribuido = corretores_lib.corr_filtros(matriz,
                                                                                    corretores_disponiveis,
                                                                                    list(corretores_ativos[
                                                                                             'cpf'].unique()),
                                                                                    filtro_plantao,
                                                                                    corretor_limiar, bases,
                                                                                    grau_justica,
                                                                                    plantao=config.PLANTAO)
    return corretores_lib.recommender(matriz_final, bases, redistribuido)


def motor_new(leads):
    corretores_ativos, corr_interno, corretores_disponiveis, filtro_plantao = obter_corretores()

    limiar_param = aux_function.limiar_param(corr_interno, filtro_plantao, fixo=False, limiar_fixo=0.04,
                                             plantao=config.PLANTAO)
    grau_justica = 0.1
    ano = datetime.today().year
    mes = datetime.today().month
    dia = datetime.today().day
    data = str(ano) + str(mes) + str(dia)
    today = str(datetime.today())[:10]
    # anomes = str(ano)+str(mes)
    date_today = dt.date.today()
    date_inicial = date_today + dt.timedelta(days=-30)

    xvar = features['xvar']
    cat = features['cat']
    num = features['num']

    df = pd.DataFrame(data=leads)

    if 'FL_CLIENTE_ATIVO' not in df.columns:
        df['FL_CLIENTE_ATIVO'] = False

    logging.basicConfig(filename=config.path_2 + '/' + 'log_motor_' + data + '.log', filemode='a',
                        format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.warning(len(df))

    # Prepara dados leads
    print("Prepara dados inicio" + str(datetime.now()))
    df = corrigir_dados_leads(leads)
    df = prepara_dados.data_prep(df)

    leads_recomendacao = []
    for i in (range(len(df))):
        df_1 = pd.DataFrame(df.loc[i, :]).T
        try:

            # Previsão propensão a venda
            classe, proba, origens_novas = estimar_propensao_venda(df_1, num, cat, xvar)
            df_1['score'] = proba[:, 1]
            df_1['flag_previsao'] = classe
            df_1['Score_xgboost_bin'] = aux_function.Score_bins(df_1['score'])
            df_1['age_bins'] = aux_function.Age_bins(df_1['DATA_NASCIMENTO'])

            if origens_novas['NM_ORIGEM'] != set():
                print('Nova origem não identificada:', origens_novas['NM_ORIGEM'])
                lista_novas_origens.append(list(origens_novas['NM_ORIGEM'])[0])

            # BeFair
            corr_full, corretor_limiar = corretores_lib.fair_motor(base_fair_motor, limiar_param)

            bases = prepara_dados.data_cluster_prep(df_1, features_cluster)
            recomendacao = recomendar_corretor(corretores_disponiveis, corretores_ativos, filtro_plantao,
                                               corretor_limiar, bases, grau_justica)
            if len(recomendacao) == 0:
                raise ValueError
            recomendacao = recomendacao.replace(np.nan, pd.DataFrame(corretores_ativos['cpf'].sample(1)).iat[0, 0])

            leads_recomendacao.append(recomendacao)

            print("lead", df_1['ID_LEAD'].iloc[0], "distribuição otimizada")

        except (IndexError, ValueError):
            # LIMITAÇÃO DE LEADS DIARIO
            # BeFair
            corr_full, corretor_limiar = corretores_lib.fair_motor(base_fair_motor, limiar_param)

            corretores_ativos2 = corretores_ativos
            corretores_ativos2 = corretores_ativos2[corretores_ativos2['cpf'].isin(corr_interno)]
            corretores_ativos2 = corretores_ativos2[~corretores_ativos2['cpf'].isin(corretor_limiar)]

            # TODO Leads redistribuidos são tratados pela aplicação?
            try:
                r = enviados_count_corretor_redistribuicao[
                    enviados_count_corretor_redistribuicao.data_referencia == today].groupby('ID_LEAD')[
                    'CPF_CORRETOR'].apply(list)
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
            print("lead", df_1['ID_LEAD'].iloc[0], "distribuição randômica")

        leads_recomendacao_pd = pd.concat([item for item in leads_recomendacao])
        leads_recomendacao_pd.to_csv(
            config.path_enviados + '/' + 'recomendacao_' + str(ano) + str(mes) + str(dia) + '.csv', sep=';', index=None)

        dados_empilhados_recebidos = historico.dados_recebidos(historico_recebidos)
        dados_empilhados_enviados = historico.dados_enviados(historico_enviados)

        dados_empilhados_motor = dados_empilhados_enviados[
            dados_empilhados_enviados["data"] > int(date_inicial.strftime('%Y%m%d'))]
        dados_empilhados_motor = dados_empilhados_motor[dados_empilhados_motor["ID_LEAD"] > 0]

        dados_empilhados_recebidos.to_csv(config.path_recebidos_historico + '/historico_recebidos.csv', index=False)
        dados_empilhados_enviados.to_csv(config.path_enviados_historico + '/historico_enviados.csv', index=False)
        dados_empilhados_motor.to_csv(config.path_fair_motor + '/dados_empilhados_motor.csv', index=False)

    recomendacao = pd.concat([item for item in leads_recomendacao])

    env = recomendacao[['ID_LEAD', 'CPF_CORRETOR']]
    env['data_referencia'] = today
    env_re = enviados_count_corretor_redistribuicao.append(env, ignore_index=True)[
        ['ID_LEAD', 'CPF_CORRETOR', 'data_referencia']]
    env_re = env_re[env_re.data_referencia == today]
    env_re.to_csv(config.path_3 + '/' + 'enviados_count_corretor_redistribuicao.csv')
    recomendacao.to_csv(config.path_recomendacao + '/recomendacao_' + str(ano) + str(mes) + str(dia) + '.csv', sep=';',
                        index=None)
    nome_exp = 'recomendacao_' + str(ano) + str(mes) + str(dia) + '.csv'

    # salva arquivos no histórico e limpa pasta de controle

    historico.salva_e_limpa_pasta_de_controle(config.path_recebidos, config.path_recebidos)
    historico.salva_e_limpa_pasta_de_controle(config.path_enviados + '/enviados_motor', config.path_enviados)

    # Bot Teams
    chamar_bot_reams(recomendacao)

    # aciona o script de retreino
    joblib.dump(lista_novas_origens, config.path_3 + '/' + 'lista_novas_origens.joblib.dat')
    return nome_exp


def motor(pay):
    corretores_ativos, corr_interno, corretores_disponiveis = corretores_lib.corr_ativos()

    if config.PLANTAO:
        filtro_plantao = corretores_lib.filtro_plantao()
    else:
        filtro_plantao=[]
    limiar_param = aux_function.limiar_param(corr_interno, filtro_plantao, fixo=False, limiar_fixo=0.04, plantao=config.PLANTAO)

    grau_justica = 0.1


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

    df = prepara_dados.data_prep(df)
    leads_recomendacao = []

    for i in (range(len(df))):
        df_1 = pd.DataFrame(df.loc[i,:]).T
        try:
            origens_novas, df_le = prepara_dados.data_clean(df_1, cat, num)
            if origens_novas['NM_ORIGEM'] != set():
                print('Nova origem não identificada:', origens_novas['NM_ORIGEM'])
                base_teste_ = base_teste.append(pd.DataFrame(df_1))
                lista_novas_origens.append(list(origens_novas['NM_ORIGEM'])[0])
                origens_novas = set()

            # Previsão propensão a venda
            df_le_2 = preprocessor.transform(df_le[xvar])
            df_le_2 = pd.DataFrame(df_le_2)
            df_le_2.columns = num+cat
            predito = xgboost_loaded_model.predict_proba(df_le_2[xvar].values)
            df_1['score'] = predito[:,1]
            df_1['flag_previsao'] = xgboost_loaded_model.predict(df_le_2[xvar].values)
            df_1['Score_xgboost_bin'] = aux_function.Score_bins(df_1['score'])
            df_1['age_bins'] = aux_function.Age_bins(df_1['DATA_NASCIMENTO'])

            # BeFair
            corr_full, corretor_limiar = corretores_lib.fair_motor(base_fair_motor, limiar_param)

            bases = prepara_dados.data_cluster_prep(df_1, features_cluster)
            corr_ativos = list(corretores_ativos['cpf'].unique())
            matriz_final, corretores_filtrados, redistribuido = corretores_lib.corr_filtros(matriz, corretores_disponiveis, corr_ativos, filtro_plantao,
                                                                                            corretor_limiar, bases, grau_justica, plantao=config.PLANTAO)

            recomendacao = corretores_lib.recommender(matriz_final, bases, redistribuido)
            if len(recomendacao)==0:
                raise ValueError
            recomendacao = recomendacao.replace(np.nan, pd.DataFrame(corretores_ativos['cpf'].sample(1)).iat[0,0])

            leads_recomendacao.append(recomendacao)

            print("lead", df_1['ID_LEAD'].iloc[0],"distribuição otimizada")

        except (IndexError, ValueError):
            # LIMITAÇÃO DE LEADS DIARIO

            # BeFair
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
            print("lead", df_1['ID_LEAD'].iloc[0], "distribuição randômica")

        leads_recomendacao_pd = pd.concat([item for item in leads_recomendacao])
        leads_recomendacao_pd.to_csv(config.path_enviados+'/'+'recomendacao_'+str(ano)+str(mes)+str(dia)+'.csv', sep=';', index=None)

        dados_empilhados_recebidos = historico.dados_recebidos(historico_recebidos)
        dados_empilhados_enviados = historico.dados_enviados(historico_enviados)

        dados_empilhados_motor = dados_empilhados_enviados[dados_empilhados_enviados["data"]>int(date_inicial.strftime('%Y%m%d'))]
        dados_empilhados_motor = dados_empilhados_motor[dados_empilhados_motor["ID_LEAD"]>0]

        dados_empilhados_recebidos.to_csv(config.path_recebidos_historico+'/historico_recebidos.csv', index=False)
        dados_empilhados_enviados.to_csv(config.path_enviados_historico +'/historico_enviados.csv', index=False)
        dados_empilhados_motor.to_csv(config.path_fair_motor+'/dados_empilhados_motor.csv', index=False)

    recomendacao = pd.concat([item for item in leads_recomendacao])
    # print("for variaveis predição "+str(datetime.now()))

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

    # aciona o script de retreino
    joblib.dump(lista_novas_origens, config.path_3+'/'+'lista_novas_origens.joblib.dat')
    return nome_exp