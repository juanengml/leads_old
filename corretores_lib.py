import requests
import json
import pandas as pd
import config
from datetime import datetime, timedelta
import numpy as np
import config
import joblib
import aux_function
import random
#FILES-----------------------------
from artefatos import capacity_corretor, profissao_lista_representativa
from artefatos import MIDIA_VEICULO_lista_representativa, Listas, xgboost_loaded_model
from artefatos import lista_novas_origens, ddd, idhm, matriz, corretores
from artefatos import categoria_treino, base_fair_motor, enviados_count_corretor_redistribuicao
from time import gmtime, strftime

today = str(datetime.today())[:10]
ano = datetime.today().year

mes = datetime.today().month

dia = datetime.today().day

hora = datetime.today().hour

minut = datetime.today().minute

seg = datetime.today().second

data_recebido = f'{str(mes)}-{str(dia)}-{str(ano)}--{str(hora)}-{str(minut)}-{str(seg)}'

def corr_ativos():
    '''
    Lista os corretores do canal interno ativos e disponíveis

    Parameters
    ----------
    corretores_interno : DataFrame (Colunas requeridas: 'CPF')
        Informações dos corretores do canal interno

    Returns
    -------
    corretores_ativos : DataFrame (coluna de cpfs)
        Retorna os cpfs dos corretores ativos.
    corr_interno : list
        Retorna os cpfs dos corretores do canal interno
    corretores_disponiveis : DataFrame
        Retorna os corretores do canal interno ativos (disponíveis)

    '''
    response = requests.request("POST", config.URL_CORRETOR, headers=config.HEADERS, data={})

    dados_corretores_ativos = json.loads(response.content)

    corretores_ativos = pd.DataFrame(dados_corretores_ativos)
    corretores_ativos.to_csv('corretores_ativos.csv', index=False)
    corretores_ativos = corretores_ativos.drop_duplicates(subset="cpf")
    
    corretores_ativos['capacity_restante'] = corretores_ativos['quantidadeLeads'] - corretores_ativos['leadsDistribuidos']
    corretores_ativos['capacity_restante_redistribuicao'] = corretores_ativos['quantidadeRedistribuicao'] - corretores_ativos['leadsRedistribuido'] 

    #--------------- CORRETORES INTERNOS E ATIVOS ------------------------------------------------------------------------------
    corretores_interno = corretores_ativos[corretores_ativos.canal=='INTERNO']
    #remove corretores que preencheram o capacity ou que ultrapassaram o limite de redistribuição
    corretores_interno = corretores_interno[corretores_interno.capacity_restante>0]
    corretores_interno = corretores_interno[corretores_interno.capacity_restante_redistribuicao>0]

    
    corretores_interno["CPF"] = pd.to_numeric(corretores_interno["cpf"], errors='coerce')

    corr_interno = list(corretores_interno.CPF.unique())

    corretores_disponiveis = corretores_ativos[corretores_ativos["cpf"].isin(corr_interno)]
    
    ## verifica se o corretor é de SP lista da jessica---------------------------------------------
    corretore_canal_interno = pd.read_excel('historico_bi/consultores_canal_interno.xlsx')
    corretore_canal_interno=list(corretore_canal_interno.CPF.unique())
    corretores_disponiveis = corretores_disponiveis[corretores_disponiveis["cpf"].isin(corretore_canal_interno)]
    # print('corretores_disponiveis')
    corr_interno = corretore_canal_interno
    # print('corr_interno')
    corretores_ativos = corretores_ativos[corretores_ativos["cpf"].isin(corretore_canal_interno)]
    # print('corretores_ativos')
    ###---------------------------------------------------------------------------------
    
    corretores_disponiveis.to_csv(config.path_2+'/'+data_recebido+'corretores_disponível_ativo.csv')
    
    return corretores_ativos, corr_interno, corretores_disponiveis
    


def capacity_restante(capacity_corretor):
    '''
    

    Parameters
    ----------
    capacity_corretor : DataFrame (colunas requeridas: 'CPF_CORRETOR, capacity, Corretor')
        Contém estudos relacionados ao capacity do corretor. A coluna capacity indica o capacity máximo do corretor    

    Returns
    -------
    capacity_restante : DataFrame
        Indica o número de leads entregues no dia via focus leads e cognitivo, além de calcular o capacity 
        restante do corrretor

    '''
    res = requests.post(config.URL_LEADS, allow_redirects=False, json={"DATA_INICIO_CAPTURA": today+' 00:00',"DATA_FIM_CAPTURA": today+' 23:59'}, headers = config.HEADERS)
    res_2 = res.json()
    corretores_api = []
    for i  in res_2:
        if i['corretor'] is not None:
            corretores_api.append(i)
    corretores_resumido = pd.DataFrame({})
    corretores_resumido["nome"] = [i['corretor']['nome'] for i in corretores_api]
    corretores_resumido["CPF"] = [i['corretor']['cpf'] for i in corretores_api]
    FL_COGNITIVO = []
    for i in corretores_api:
        try:
            FL_COGNITIVO.append(i['lead']['FL_COGNITIVO'])
        except:
            FL_COGNITIVO.append(False)
    corretores_resumido["FL_COGNITIVO"] = FL_COGNITIVO
    corretores_count = corretores_resumido.groupby(['CPF','FL_COGNITIVO']).count()['nome'].reset_index()
    corretores_count = pd.pivot_table(corretores_count, values='nome', index=['CPF'], columns='FL_COGNITIVO', aggfunc=np.sum).replace(np.nan,0)
    corretores_leads_trabalhados = corretores_resumido[['nome', 'CPF']].drop_duplicates(['CPF'])
    corretores_leads_trabalhados = pd.merge(corretores_leads_trabalhados, corretores_count, how='inner', on='CPF').rename(columns={False:'Focus_lead', True:'Cognitivo'})
    if 'Cognitivo' not in corretores_leads_trabalhados.columns:
        corretores_leads_trabalhados['Cognitivo'] = 0 
    capacity_restante = pd.merge(capacity_corretor[['CPF_CORRETOR','capacity','Corretor']], corretores_leads_trabalhados, how='left', left_on='CPF_CORRETOR', right_on='CPF')[['CPF_CORRETOR', 'Corretor', 'Focus_lead', 'Cognitivo', 'capacity']].replace(np.nan, 0)
    capacity_restante['capacity_restante'] = capacity_restante['capacity']-(capacity_restante['Cognitivo']+capacity_restante['Focus_lead'])
    
    return capacity_restante


def fair_motor(base_fair_motor, limiar_param):  
    '''
    PROGRAMA FAIR - busca leads trabalhados nos últimos dias de cada corretor, verifica a quantidade relativa trabalhado 
    de cada corretor ao total e retira corretores com proporção acima de um limiar especificado
    Parameters
    ----------
    base_fair_motor : DataFrame
        Base com os últimos leads trabalhados de cada corretor
    limiar_param : float
        parâmetro de limiar do fair

    Returns
    -------
    corr_full : list
        corretores que estavam em base_fair_motor
        
    corretor_limiar : list
        CPF's dos corretores que ultrapassaram o limiar fair

    '''

    corretor_full = base_fair_motor.groupby("CPF_CORRETOR").count().reset_index()[["CPF_CORRETOR","ID_LEAD"]]
    
    # ---------------------------------- FAIR ----------------------------------------------------
    
    corretor_full["%LEADS"] = corretor_full["ID_LEAD"]/(corretor_full["ID_LEAD"].sum())
    qtd_corr_dist = len(corretor_full)-1
    #leads_dist = corretor_full["ID_LEAD"].sum()

    if qtd_corr_dist > 0:
        limiar = limiar_param
    else:
        limiar = 0

    corretor_limiar = corretor_full[corretor_full["%LEADS"]>limiar]
    corretor_limiar = list(corretor_limiar.CPF_CORRETOR.unique())
    
    # --------------------------------------------------------------------------------------------
    #corretor_full = pd.merge(corretor_full, capacity_restante[['CPF_CORRETOR', 'capacity_restante']], how='left', on='CPF_CORRETOR').fillna(2)

    #corretor_full = corretor_full[(corretor_full["ID_LEAD"]>=corretor_full['capacity_restante'])]
    #corretor_full = corretor_full[(corretor_full["ID_LEAD"]>=capacity_diario ) | (corretor_full["%LEADS"]>limiar)]
    
    corr_full = list(corretor_full.CPF_CORRETOR.unique())
    return corr_full, corretor_limiar


def filtro_plantao(increment_time=5):
    today = str(datetime.today())[:10]    
    body = {'DataHoraInicio':today+' 00:00:00', 'DataHoraFim':today+' 23:59:59'}
    response = requests.request('POST', config.URL_PLANTAO, headers=config.HEADERS, data=body)
    timestamp = datetime.timestamp(datetime.now() + timedelta(minutes=increment_time))

    output = json.loads(response.content)
    output = filter(lambda x: x['DataHoraFim'] > timestamp, output)
    output = filter(lambda x: x['status'] == 'Em andamento', output)
    
    corretores = [item for sublist in output for item in sublist['Corretores']]
    corretores = list(filter(lambda x: x['pausado']==False, corretores))
    
    corretor_plantao = list(filter(lambda x: x['canal']=='INTERNO', corretores))
    corretor_plantao_cpf = list(map(lambda x: float(x['cpf']), corretor_plantao))
    
    log_corretor_plantao = pd.DataFrame({'cpf': list(map(lambda x: x['cpf'], corretores)),
                                         'canal': list(map(lambda x: x['canal'], corretores))})
    
    log_corretor_plantao['Filtro'] =  log_corretor_plantao.cpf.apply(
        lambda x: x in corretor_plantao_cpf)

    get_column = lambda col: list(map(lambda x: x[col], corretor_plantao))
    corretor_plantao_df = pd.DataFrame({'corretor_plantao': corretor_plantao_cpf,
                                        'leadsDistribuidos': get_column('leadsDistribuidos'),
                                        'leadsRedistribuido': get_column('leadsRedistribuido'),
                                        'quantidadeLeads': get_column('quantidadeLeads'),
                                        'quantidadeRedistribuicao': get_column('quantidadeRedistribuicao'),
                                        'uf_corretor': get_column('uf')})
    
    corretor_plantao_df['capacity_restante'] = corretor_plantao_df['quantidadeLeads'] - \
        corretor_plantao_df['leadsDistribuidos']
    corretor_plantao_df['capacity_restante_redistribuicao'] = corretor_plantao_df['quantidadeRedistribuicao'] - \
        corretor_plantao_df['leadsRedistribuido']
    corretor_plantao_df = corretor_plantao_df[corretor_plantao_df.capacity_restante>0]
    corretor_plantao_df = corretor_plantao_df[corretor_plantao_df.capacity_restante_redistribuicao>0]
    
    log_corretor_plantao = pd.merge(log_corretor_plantao,
                                    corretor_plantao_df[['corretor_plantao', 'capacity_restante']],
                                    left_on='cpf', right_on='corretor_plantao', how='left')
    log_corretor_plantao = log_corretor_plantao.drop('corretor_plantao', axis=1)
    
    # tabele de corretores canal interno envida pela Jessica ------------------------------------
    corretore_canal_interno = pd.read_excel('historico_bi/consultores_canal_interno.xlsx')
    corretore_canal_interno=list(corretore_canal_interno.CPF.unique())
    corretor_plantao_df = corretor_plantao_df[corretor_plantao_df["corretor_plantao"].isin(
        corretore_canal_interno)]
    #------------------------------------------------------------------------------------------------------------
    return list(corretor_plantao_df['corretor_plantao'])



def corr_filtros(matriz, corretores_disponiveis, corr_ativos, filtro_corretores_plantao, corretor_limiar, bases, grau_justica, plantao=False):
    '''
    Aplica os diversos filtros de corretores na matriz de recomendação.

    Parameters
    ----------
    matriz : DataFrame
        Tabela que asscocia uma nota de cada corretor para cada cluster de lead
    corr_interno : lista
        Lista de cpf's de corretores do canal interno
    corr_ativos : lista
        lista de cpf's de corretores 
    corretor_limiar : Lista
        Lista de cpf's de corretores que ultrapassaram o limiar
    bases : DataFrame com 1 linha
        base de output que com o cluster do lead (coluna cluster) 
    capacity_corretor : DataFrame (colunas requeridas: 'CPF_CORRETOR, capacity, Corretor')
        Contém estudos relacionados ao capacity do corretor. A coluna capacity indica o capacity máximo do corretor 
    grau_justica : float
        Parâmetro de nível de discretização das notas dos corretores

    Returns
    -------
    matriz_final : DataFrame
        Matriz filtrada
    corretores_filtrados: DataFrame
        corretores que passaram pelo filtro e podem ser recomendados
    redistribuido : bool
        Flag que indica se o lead está sendo redistribuido

    '''
    corr_interno = list(set(corretores_disponiveis.cpf)) 
    
    if plantao:
        if len(filtro_corretores_plantao)==0:
            pass
        else:
            matriz = matriz[matriz['corretor_id'].isin(filtro_corretores_plantao)]
            if len(matriz)==0:
                raise IndexError
    else:
        matriz = matriz[matriz["corretor_id"].isin(corr_interno)]#filtra corretores canal interno
        matriz = matriz[matriz["corretor_id"].isin(corr_ativos)] #filtro corretores ativos
        matriz = matriz[~matriz["corretor_id"].isin(corretor_limiar)] #filtro corretor limiar
        
    #matriz = matriz[~matriz["corretor_id"].isin(corr_full)] #limite diario leads
    matriz.sort_values(by='corretor_id', inplace=True)
    matriz = pd.merge(matriz, bases[['ID_LEAD', 'cluster']], how='left', on='cluster').dropna()

    #corretores = capacity_corretor[capacity_corretor["CPF_CORRETOR"].isin(corr_ativos)] #filtro corretores ativos
    
    corretores = corretores_disponiveis[corretores_disponiveis['cpf'].isin(corr_interno)] #filtro corretores interno
    corretores = corretores[~corretores['cpf'].isin(corretor_limiar)] #filtro corretor limiar
    
    ## Filtra corretores que estão em uma tabela fornecida pela Jessica-------------------------------
    corretore_canal_interno = pd.read_excel('historico_bi/consultores_canal_interno.xlsx')
    corretore_canal_interno=list(corretore_canal_interno.CPF.unique())
    corretores= corretores[corretores["cpf"].isin(corretore_canal_interno)]
    ##------------------------------------------------------------------------------------------------
    
    #caso o lead tenha sido redistribuido, exclui corretores que jpa trabalharam nesse lead da recomendação
    try:
        r = enviados_count_corretor_redistribuicao[enviados_count_corretor_redistribuicao.data_referencia==today].groupby('ID_LEAD')['CPF_CORRETOR'].apply(list)

        if bases['ID_LEAD'][0] in list(r.index):
            redistribuido = True
            corretores = corretores[~corretores['CPF_CORRETOR'].isin(r[bases['ID_LEAD'][0]])]
        else:
            redistribuido = False
    except: 
        redistribuido = False

    #corretores = corretores[~corretores['CPF_CORRETOR'].isin(corr_full)] #limite diario leads
    corretores_filtrados = corretores[corretores.cpf.isin(matriz.corretor_id.unique())]
    matriz = matriz[['ID_LEAD','corretor_id','estimativa']].pivot(index='ID_LEAD', columns='corretor_id')
           
    matriz_justa = pd.DataFrame()
    GRAU_DE_JUSTICA = grau_justica

    for lead in matriz.index:
        cluster_max = matriz.loc[lead,:].sort_values(ascending=False).max()
        cluster_min = matriz.loc[lead,:].sort_values(ascending=False).min()
        cluster_range = np.arange(cluster_min,cluster_max,GRAU_DE_JUSTICA)
        matriz_justa = matriz.estimativa.loc[lead,:].apply(lambda y: cluster_range[abs(cluster_range-y).argmin()])
    matriz_justa = pd.DataFrame(matriz_justa).T
    matriz_justa = matriz_justa.reset_index()
    matriz_justa.rename(columns={'index':'Leads'}, inplace=True)

    matriz_justa.fillna(1, inplace=True)
    matriz_final = matriz_justa.copy()
    return matriz_final, corretores_filtrados, redistribuido


def recommender(matriz_final, bases, redistribuido):
    '''
    Motor de recomendação, avalia os corretoreas da matriz e seleciona aquele com a maior nota 
    entre els e o cluster do lead

    Parameters
    ----------
    matriz_final : DAtaFrame
        Notas de recomendação para os corretores filtrados
    bases : DataFrame com 1 linha
        base de output que com o cluster do lead (coluna cluster) 
    redistribuido : bool
        indica se o lead foi redistribuido

    Returns
    -------
    recomendacao : DataFrame
        Indica os três corretores com as maiores notas e o score do lead selecionado. O rating varia de 1 a 5.

    '''
    lead_max = len(matriz_final)
    output = [0]*lead_max
    output_2 = [0]*lead_max
    output_3 = [0]*lead_max
    nota_dist_1, nota_dist_2, nota_dist_3 = [0]*lead_max,[0]*lead_max,[0]*lead_max 
    #output
    
    # se a nota atual for maior que a máxima vista anteriormente
    #lead_count=1
    # print("inicio do for de recommender "+str(datetime.now()))
    for lead_count in range(lead_max): # enquanto o contador de leads for menor ou igual ao máximo de leads
    
        corr_dist = matriz_final.columns[aux_function.randargmax(matriz_final.iloc[lead_count,1::])+1] #nome do corretor
        nota_dist1 = matriz_final.iloc[lead_count, 1::].sort_values(axis=0)[-1]
        try:
            corr_dist2 = matriz_final.iloc[lead_count, 1::].sort_values(axis=0).index[-2]
            nota_dist2 = matriz_final.iloc[lead_count, 1::].sort_values(axis=0)[-2]
            corr_dist3 = matriz_final.iloc[lead_count, 1::].sort_values(axis=0).index[-3]
            nota_dist3 = matriz_final.iloc[lead_count, 1::].sort_values(axis=0)[-3]
        except IndexError:
            corr_dist2 = corr_dist
            nota_dist2 = nota_dist1
            corr_dist3 = corr_dist
            nota_dist3 = nota_dist1  
    # remove um unidade do capacity do corretor que recebeu o lead    
    #mostra qual foi o corretor que receberam os leads das linhas da matriz
    #print(corr_dist, "recebe o lead", lead_count, "capacity restante:", capacity_historico.iloc[capacity_historico[capacity_historico["corretor"]==corr_dist].index[0],3]) 
        #print("Lead", lead_count, "é distribuido para", corr_dist,"capacity restante:", capacity_historico.iloc[capacity_historico[capacity_historico["CPF"]==corr_dist].index[0],3])
        output[lead_count]=corr_dist
        output_2[lead_count]=corr_dist2
        output_3[lead_count]=corr_dist3
        nota_dist_1[lead_count] = nota_dist1
        nota_dist_2[lead_count] = nota_dist2
        nota_dist_3[lead_count] = nota_dist3
        #print("   ")
        if len(matriz_final.columns) ==1:
            #print('Os corretores excederam seu capacity')
            break
    # print("inicio concat recommender "+str(datetime.now()))
    output_pd = pd.concat([pd.Series(output), pd.Series(output_2), pd.Series(output_3), pd.Series(nota_dist_1), pd.Series(nota_dist_2), pd.Series(nota_dist_3)], axis=1)
    recomendacao = pd.merge(matriz_final["Leads"], output_pd, right_index= True, left_index=True).rename(columns={'Leads': 'lead_distribuido', 0: 'corretor_distribuicao'})

    #recomendacao = pd.merge(recomendacao, notas, left_index = True, right_index=True)
    recomendacao = pd.merge(recomendacao, bases[["ID_LEAD", "score"]], left_on = 'lead_distribuido', right_on='ID_LEAD')
    recomendacao.columns = ['ID_LEAD', 'CPF_CORRETOR', 'CPF_CORRETOR_2',
                        'CPF_CORRETOR_3', 'RATING', 'RATING_2', 'RATING_3', 'ID_lead', 'SCORE']
    recomendacao['FLAG_REDISTRIBUICAO'] = redistribuido
    recomendacao.drop(['ID_lead'], axis=1, inplace=True)
    # print("Fim dos merge de recommender "+str(datetime.now()))
    return recomendacao


def random_recommender(corretores_ativos2, df_1, plantao, filtro_corretores_plantao):
    '''
    Função de except, caso o recommender dar erro. Faz uma classificação aleatória.
    Parameters
    ----------
    corretores_ativos2 : DataFrame com uma coluna de cpfs
        Cpfs de corretores ativos
    df_1 : DataFrame com 1 linha
        linha do loop da base df_f (output da função data_prep)

    Returns
    -------
    recomendacao : DataFrame
        Recomendação aleatória (o rating dos corretores são marcados como 0)
        
    '''

    if plantao:
        recomendacao = {}
        recomendacao = pd.DataFrame(recomendacao)
        recomendacao['ID_LEAD'] = df_1['ID_LEAD']
        recomendacao['CPF_CORRETOR'] = random.choice(filtro_corretores_plantao)
        recomendacao['CPF_CORRETOR_2'] = random.choice(filtro_corretores_plantao)
        recomendacao['CPF_CORRETOR_3'] = random.choice(filtro_corretores_plantao)
        recomendacao['RATING'] = 0 #DISRIBUIÇÃO ALEATÓRIA
        recomendacao['RATING_2'] = 0 #DISRIBUIÇÃO ALEATÓRIA
        recomendacao['RATING_3'] = 0 #DISRIBUIÇÃO ALEATÓRIA 
        try:
            recomendacao['SCORE'] = df_1.score
        except:
            recomendacao['SCORE'] = 0
            
    else:
        recomendacao = {}
        recomendacao = pd.DataFrame(recomendacao)
        recomendacao['ID_LEAD'] = df_1['ID_LEAD']
        recomendacao['CPF_CORRETOR'] = pd.DataFrame(corretores_ativos2['cpf'].sample(1)).iat[0,0]
        recomendacao['CPF_CORRETOR_2'] = pd.DataFrame(corretores_ativos2['cpf'].sample(1)).iat[0,0]
        recomendacao['CPF_CORRETOR_3'] = pd.DataFrame(corretores_ativos2['cpf'].sample(1)).iat[0,0]
        recomendacao['RATING'] = 0 #DISRIBUIÇÃO ALEATÓRIA
        recomendacao['RATING_2'] = 0 #DISRIBUIÇÃO ALEATÓRIA
        recomendacao['RATING_3'] = 0 #DISRIBUIÇÃO ALEATÓRIA
        recomendacao['SCORE'] = 0
    return recomendacao