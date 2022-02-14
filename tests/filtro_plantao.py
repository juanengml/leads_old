import json
import requests
from datetime import datetime, timedelta

import pandas as pd

URL_PLANTAO = 'https://qualitech.qualicorp.com.br/focus-leads-integration/buscar-plantao-lumini'
API_KEY_HEAD = 'X-Gravitee-Api-Key'
API_KEY = '2e31146a-f2d5-42b4-a228-341b31db7652'
HEADERS = {API_KEY_HEAD:API_KEY}


def filtro_plantao_new(increment_time=5):
    today = str(datetime.today())[:10]    
    body = {'DataHoraInicio': today+' 00:00:00', 'DataHoraFim': today+' 23:59:59'}
    response = requests.request('POST', URL_PLANTAO, headers=HEADERS, data=body)
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


def filtro_plantao():
    today = str(datetime.today())[:10]
    Body = {'DataHoraInicio':today+' 00:00:00', 'DataHoraFim':today+' 23:59:59'}
    response = requests.request('POST', URL_PLANTAO, headers=HEADERS, data=Body)
    j_1 = json.loads(response.content)
    timestamp = datetime.timestamp(datetime.now())
    j_1x = [x for x in j_1 if x['DataHoraFim']>=timestamp]
    j_2 = [i['Corretores'] for i in j_1x if i['status']=='Em andamento']
    corretores=[]
    for i in j_2:
        for j in i:
            corretores.append(j)
    corretor_plantao = [float(i['cpf']) for i in corretores if (i['canal']=='INTERNO' and i['pausado']==False)]
    log_corretor_plantao = pd.DataFrame({'cpf': [i['cpf'] for i in corretores if i['pausado']==False], 'canal':[i['canal'] for i in corretores if i['pausado']==False]})
    #l = [i for i in corretores if i['cpf']==10543815420]
    #corretores_dup = log_corretor_plantao.loc[log_corretor_plantao.cpf.duplicated(),:]
    log_corretor_plantao['Filtro'] =  log_corretor_plantao.cpf.apply(lambda x: True if x in corretor_plantao else False)

    l1 = [i['leadsDistribuidos'] for i in corretores if (i['canal']=='INTERNO' and i['pausado']==False)]
    r1 = [i['leadsRedistribuido'] for i in corretores if (i['canal']=='INTERNO' and i['pausado']==False)]
    l2 = [i['quantidadeLeads'] for i in corretores if (i['canal']=='INTERNO' and i['pausado']==False)]
    r2 = [i['quantidadeRedistribuicao'] for i in corretores if (i['canal']=='INTERNO' and i['pausado']==False)]
    uf_corretor = [i['uf'] for i in corretores if (i['canal']=='INTERNO' and i['pausado']==False)]
    
    corretor_plantao_df = pd.DataFrame({'corretor_plantao':corretor_plantao, 'leadsDistribuidos':l1, 'leadsRedistribuido':r1,
                  'quantidadeLeads':l2, 'quantidadeRedistribuicao':r2,'uf_corretor':uf_corretor})
    corretor_plantao_df['capacity_restante'] = corretor_plantao_df['quantidadeLeads'] - corretor_plantao_df['leadsDistribuidos']
    corretor_plantao_df['capacity_restante_redistribuicao'] = corretor_plantao_df['quantidadeRedistribuicao'] - corretor_plantao_df['leadsRedistribuido']
    corretor_plantao_df = corretor_plantao_df[corretor_plantao_df.capacity_restante>0]
    corretor_plantao_df = corretor_plantao_df[corretor_plantao_df.capacity_restante_redistribuicao>0]
    log_corretor_plantao = pd.merge(log_corretor_plantao, corretor_plantao_df[['corretor_plantao', 'capacity_restante']], left_on='cpf', right_on='corretor_plantao', how='left')
    log_corretor_plantao = log_corretor_plantao.drop('corretor_plantao', axis=1)
    
    # tabele de corretores canal interno envida pela Jessica ------------------------------------
    corretore_canal_interno = pd.read_excel('historico_bi/consultores_canal_interno.xlsx')
    corretore_canal_interno=list(corretore_canal_interno.CPF.unique())
    corretor_plantao_df = corretor_plantao_df[corretor_plantao_df["corretor_plantao"].isin(corretore_canal_interno)]
    #------------------------------------------------------------------------------------------------------------
    return list(corretor_plantao_df['corretor_plantao'])