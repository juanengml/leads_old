import numpy as np
import pandas as pd
from datetime import datetime
import aux_function
import unidecode
import config
import regex as re
import joblib
import json
from fuzzywuzzy import fuzz
from artefatos import capacity_corretor, profissao_lista_representativa, base_teste
from artefatos import MIDIA_VEICULO_lista_representativa, Listas, xgboost_loaded_model, preprocessor_cluster
from artefatos import lista_novas_origens, ddd, idhm, matriz, corretores
from artefatos import categoria_treino, base_fair_motor, features_cluster, kmeans_model
#enviados_count_corretor_redistribuicao

def data_prep_treino(df, treino=False):
#transformação dos dados
    df['CIDADE'] = df.CIDADE.astype(str).apply(lambda x: unidecode.unidecode(x.lower())) 
    df['ds_profissao'] = df.ds_profissao.astype(str).apply(lambda x: unidecode.unidecode(x.lower()))
    df['ds_profissao'] = df.ds_profissao.replace(np.nan, 'nao definido')
    df['ds_profissao'] = df.ds_profissao.fillna('nao definido')
    df['ds_profissao'] = df.ds_profissao.replace('nan', 'nao definido')
    df['ds_profissao'] = df.ds_profissao.replace('', 'nao definido')
    df['ORIGEM_DE_MIDIA_VEICULO'] = df.ORIGEM_DE_MIDIA_VEICULO.astype(str).apply(lambda x: unidecode.unidecode(x.lower()))
    df['GRUPOORIGEM'] = df.GRUPOORIGEM.astype(str).apply(lambda x: unidecode.unidecode(x.lower()))
    df['NM_ORIGEM'] = df.NM_ORIGEM.astype(str).apply(lambda x: unidecode.unidecode(x.lower()))
    df['PRACA'] = df.PRACA.astype(str).apply(lambda x: unidecode.unidecode(x.lower()))

    df['ds_profissao'] = df.ds_profissao.replace({Listas[6][i]: Listas[7][i] for i in range(len(Listas[6]))})
    df['EntidadeLead'] = df.EntidadeLead.replace({Listas[2][i]: Listas[3][i] for i in range(len(Listas[2]))})
    df['ORIGEM_DE_MIDIA_VEICULO'] = df.ORIGEM_DE_MIDIA_VEICULO.replace({Listas[4][i]: Listas[5][i] for i in range(len(Listas[4]))})
    #primeiros 10 caracteres de midia_veiculo e midia_formato apenas. Remove numeros e caracteres especiais 
    df['ORIGEM_DE_MIDIA_VEICULO'] = df['ORIGEM_DE_MIDIA_VEICULO'].apply(lambda x : ''.join(re.findall(r'\p{L}\p{M}*+', x))).str[:10]
    df['ORIGEM_DE_MIDIA_FORMATO'] = df['ORIGEM_DE_MIDIA_FORMATO'].apply(lambda x : ''.join(re.findall(r'\p{L}\p{M}*+', x))).str[:10]
    df['EntidadeLead'] = df['EntidadeLead'].apply(lambda x : ''.join(re.findall(r'\p{L}\p{M}*+', x)))

    df['CIDADE'] = df.CIDADE.replace({Listas[0][i]: Listas[1][i] for i in range(len(Listas[0]))})

    df['FlClienteAtivo'] = df.FlClienteAtivo.replace({'False':'FALSE', 'True':'TRUE', 'VENDA':'VENDA90DIAS'})

    if treino==True:
        #algoritmo que busca por categorias similares treinadas caso não encontre a categoria da instância em particular.
        profissao_alta_representacao = df.ds_profissao.value_counts().reset_index().query('ds_profissao>=20')['index'].unique()
        MV_alta_representacao = df.ORIGEM_DE_MIDIA_VEICULO.value_counts().reset_index().query('ORIGEM_DE_MIDIA_VEICULO>=20')['index'].unique()

        joblib.dump(profissao_alta_representacao, config.path_3+'/'+'profissa☺o_lista_representativa.sav')
        joblib.dump(MV_alta_representacao, config.path_3+'/'+'MIDIA_VEICULO_lista_representativa.sav')

    dict_MV_similar = aux_function.dict_similar_f(df.ORIGEM_DE_MIDIA_VEICULO.unique(), list(MIDIA_VEICULO_lista_representativa), 80)
    dict_profissao_similar = aux_function.dict_similar_f(df.ds_profissao.unique(), list(profissao_lista_representativa), 80)
    dict_origem_similar = aux_function.dict_similar_f(df.NM_ORIGEM.unique(), list(categoria_treino['NM_ORIGEM']), 80)
    
    df['NM_ORIGEM'] = df.NM_ORIGEM.replace(dict_origem_similar)
    df['ds_profissao'] = df.ds_profissao.replace(dict_profissao_similar)
    df['ORIGEM_DE_MIDIA_VEICULO'] = df.ORIGEM_DE_MIDIA_VEICULO.replace(dict_MV_similar)
    df['ds_profissao'] = df.ds_profissao.apply(lambda x: 'pouca_representacao' if x not in profissao_lista_representativa else x)
    df['ORIGEM_DE_MIDIA_VEICULO'] = df.ORIGEM_DE_MIDIA_VEICULO.apply(lambda x: 'pouca_representacao' if x not in MIDIA_VEICULO_lista_representativa else x)

    #introdução da variável IDH
    idhm['Municipio'] = idhm.Município.apply(lambda x: unidecode.unidecode(x.lower()[:-5]))
    idhm.append({'Ranking IDHM 2010' : 'ns' , 'Município' : 'vale do paraiba', 'IDHM 2010':0.781}, ignore_index=True)
    idhm.drop_duplicates(subset=['Municipio'], keep='first', inplace=True)
    df = pd.merge(df,idhm[['Municipio', 'IDHM 2010']], left_on='CIDADE', right_on='Municipio', how='left')

    df['IDHM 2010'].fillna(df['IDHM 2010'].median(), inplace=True)
    #df['NM_ORIGEM'] = df.NM_ORIGEM.replace({'detalhe do plano':'\tdetalhe do plano'})
    return df


def data_prep(base):
    '''
    Faz a preparação das colunas, tratamento de dados faltantes, conversão da data etc
    Parameters
    ----------
    base : DataFrame
        base com informações dos leads (colunas requeridas: ['PRACA','NM_ORIGEM','FlClienteAtivo', 'GRUPO_ORIGEM', 'MIDIA_VEICULO', 
            'MIDIA_FORMATO', 'PROFISSAO','data_referencia_relatorio',
            'DATA_NASCIMENTO', 'ENTIDADE', 'MUNICIPIO', 
            'dia_de_semana', 'Hora_atribuida_dt_geracao', 'ID_LEAD', 'data_referencia_relatorio', 'praça'])

    Returns
    -------
    df_f : DataFrame
        Base com as colunas preparadas para a etapa de transformação

    '''
    df = base.replace("", np.nan)
    df = aux_function.limpeza_whitespace(df)
    try:
        df['ddd'] = df.TELEFONE_PRINCIPAL.astype(str).str[:2]
    except:
        df['ddd'] = df.TELEFONE_SECUNDARIO.astype(str).str[:2]
        
    ddd.ddd = ddd.ddd.astype(str)
    df = pd.merge(df, ddd, how='left', on='ddd' )
    
    #praça
    df['DATA_NASCIMENTO']=df['DATA_NASCIMENTO'].apply(lambda x: np.nan if x is None else pd.to_datetime(x, errors="coerce"))
    
    
    #data_referencia_relatorio
    
    df = df[df['DH_CAPTURA_LEAD_ORIGEM']>0]
    df['data_referencia_relatorio'] =  df['DH_CAPTURA_LEAD_ORIGEM'].apply(lambda x: datetime.fromtimestamp(x/1000))
    df['data_referencia_relatorio']
    
    df['DATA_NASCIMENTO'] = aux_function.diferenca_tempo(df['data_referencia_relatorio'], df['DATA_NASCIMENTO'], formato='%Y-%m-%d')/np.timedelta64(1, 'D')/365
    df['dia_de_semana'] = pd.to_datetime(df.data_referencia_relatorio).dt.dayofweek
    df['Hora_atribuida_dt_geracao'] = pd.to_datetime(df['data_referencia_relatorio']).dt.hour
    df['praça'] = df.praça.astype(str).apply(lambda x: unidecode.unidecode(x.lower()))
    df['praça'] = df['praça'].replace({'nan':np.nan,'NaN':np.nan, 'NA':np.nan})
    df['praça'] = df['praça'].replace(np.nan,'demais')
    df['PRACA'] = df['praça']
    df['FlClienteAtivo'] = df['FL_CLIENTE_ATIVO'].replace({False:'FALSE', np.nan:'FALSE', True:'TRUE'})
    #filtros da área de markting. São considerados apenas praças com metas. 
    praca = ['AL', 'AM', 'BA', 'CE', 'DF', 'GSP', 'MA', 'MG', 'PA', 'PB', 'PE',
            'PETRÓPOLIS', 'PR', 'RJ', 'RN', 'RS', 'SC', 'SE', 'SOROCABA', 'SPI_Campinas', 'VALE DO PARAÍBA']
    #grupo origem
    #grupo_origem = ['Captura', 'Filipeta', 'Hotline', 'Solicitaçăo', 'Mobile', 'Detalhe do Plano']
    #flag cliente ativo
    #flciente_ativo = ['FALSE','VENDA90DIAS']
    df = aux_function.limpeza_whitespace(df)
    df['filtro_praca'] = df.PRACA.apply(lambda x: 1 if x in praca else 0)
    #df['filtro_origem'] = df.GRUPOORIGEM.apply(lambda x: 1 if x in grupo_origem else 0)
    #df['filtro_cliente_ativo'] = df.FlClienteAtivo.apply(lambda x: 1 if x in flciente_ativo else 0)
    #df['Filtro'] = (df['filtro_praca']==1)#&df['filtro_origem']&df['filtro_cliente_ativo']
    df_f = df.copy()
    return df_f

def data_clean(df_1, cat, num):
    '''
    Realiza a correção de strings e transformação para a etapa de preprocessamento

    Parameters
    ----------
    df_1 : DataFrame com 1 linha
        linha do loop da base df_f (output da função data_prep)
    cat : list
        Variáveis preditoras categoricas
    num : list
        Variáveis preditoras numéricas

    Returns
    -------
    df_le : DataFRame (1 linha)
        linha do loop limpa e transformada para a etapa de preprocessamento

    '''
    xvar = cat+num
    
    df_1 = data_prep_treino(df_1)
        #transformação dos dados
    df_2 = df_1[xvar]
    df_2 = df_2.replace('', np.nan)
    
    categoria_motor = {'NM_ORIGEM':set(df_2.NM_ORIGEM), 'ORIGEM_DE_MIDIA_VEICULO':set(df_2.ORIGEM_DE_MIDIA_VEICULO),
                                                  'ds_profissao':set(df_2.ds_profissao), 'EntidadeLead':set(df_2.EntidadeLead),
                                                  'ORIGEM_DE_MIDIA_FORMATO':set(df_2.ORIGEM_DE_MIDIA_FORMATO)}
    origens_novas = {'NM_ORIGEM': categoria_motor['NM_ORIGEM']-categoria_treino['NM_ORIGEM']}
    
    df_2[cat] = df_2[cat].fillna("NA").astype("str")
    df_2[cat] = df_2[cat].replace('nan',"NA").astype("str")
    df_2[num] = df_2[num].fillna('-1').astype('float64')
    #origem_validas = joblib.load('origem_validas.sav')
    df_2.NM_ORIGEM = df_2.NM_ORIGEM.apply(lambda x: 'solicitacao' if x not in list(categoria_treino['NM_ORIGEM']) else x)
    df_2.ORIGEM_DE_MIDIA_FORMATO = df_2.ORIGEM_DE_MIDIA_FORMATO.apply(lambda x: 'NA' if x not in list(categoria_treino['ORIGEM_DE_MIDIA_FORMATO']) else x)
    df_2.ds_profissao  = df_2.ds_profissao.apply(lambda x: 'pouca_representacao' if x not in list(categoria_treino['ds_profissao']) else x) 
    df_2.EntidadeLead = df_2.EntidadeLead.apply(lambda x: 'NA' if x not in list(categoria_treino['EntidadeLead']) else x)
    
    
    '''
     'ORIGEM_DE_MIDIA_VEICULO': categoria_motor['ORIGEM_DE_MIDIA_VEICULO']-categoria_treino['ORIGEM_DE_MIDIA_VEICULO'],
                             'ds_profissao': categoria_motor['ds_profissao']-categoria_treino['ds_profissao'],
                             'EntidadeLead': categoria_motor['EntidadeLead']-categoria_treino['EntidadeLead'],
                             'ORIGEM_DE_MIDIA_FORMATO': categoria_motor['ORIGEM_DE_MIDIA_FORMATO']-categoria_treino['ORIGEM_DE_MIDIA_FORMATO'],
                             'GRUPOORIGEM': categoria_motor['GRUPOORIGEM']-categoria_treino['GRUPOORIGEM']
    '''                         
    
    
    df_le = df_2.copy()
    return origens_novas, df_le

def data_cluster_prep(df_1, features_cluster):
    '''
    Faz a preparação dos dados e indica o cluster do lead

    Parameters
    ----------
    df_1 : DataFrame com 1 linha
        linha do loop da base df_f (output da função data_prep)
    cat_cluster : lista
        variáveis categóricas de clusterização
    num_cluster : lista
        variáveis numéricas de clusterização
    others_cluster : lista
        outras variáveis a serem adicionadas na base de output

    Returns
    -------
    bases : DataFrame com 1 linha
        base de output que com o cluster do lead (coluna cluster)

    '''


    xvar_cluster = features_cluster['xvar_cluster']
    others = ['ID_LEAD', 'data_referencia_relatorio']
    df_cluster = df_1[xvar_cluster+others]
    cat_cluster = features_cluster['cat']
    num_cluster = features_cluster['num']

    #clusteriza por praça
    #Primeiro faz a separação por praças. DEpois, baseado na representatividade da praça, escolhe o número de clusters, 
    #que deve conter 500 leads
    
    df_cluster = df_cluster.replace('', np.nan)
    
    dict_praca = {'gsp':1, 'spi_campinas':2, 'spi': 3, 'sorocaba':4, 'spl':5,'vale do paraiba':6, 'rj':7,
       'petropolis':8, 'mg':9, 'es': 10, 
        'ba':11, 'se':12, 'al':13,'pe':14, 'rn':15, 'ce':16, 'ma':17, 'pi':18, 'pb':19,
        'rs':30, 'pr':31, 'sc':32,
        'df':20, 'go':21, 'mt':22, 'ms':23, 
        'am':40, 'ro':41, 'ap':42, 'to':43, 'ac':44, 'rr':45, 'pa':46,
       'demais':99}

    df_cluster.PRACA = df_cluster.PRACA.replace(dict_praca).astype(int)
    df_cluster[cat_cluster] = df_cluster[cat_cluster].fillna("NA").astype("str")
    df_cluster[cat_cluster] = df_cluster[cat_cluster].replace('nan',"NA").astype("str")
    df_cluster[num_cluster] = df_cluster[num_cluster].fillna('-1').astype('float64')
    
    dict_profissao_similar = aux_function.dict_similar_f(df_cluster.ds_profissao.unique(), list(categoria_treino['ds_profissao']), 80)
    dict_origem_similar = aux_function.dict_similar_f(df_cluster.NM_ORIGEM.unique(), list(categoria_treino['NM_ORIGEM']), 80)

    df_cluster['NM_ORIGEM'] = df_cluster.NM_ORIGEM.replace(dict_origem_similar)
    df_cluster.NM_ORIGEM = df_cluster.NM_ORIGEM.apply(lambda x: 'solicitacao' if x not in list(categoria_treino['NM_ORIGEM']) else x)

    df_cluster['ds_profissao'] = df_cluster.ds_profissao.replace(dict_profissao_similar)
    df_cluster['ds_profissao'] = df_cluster.ds_profissao.apply(lambda x: 'nao definido' if x not in list(categoria_treino['ds_profissao']) else x)



    # In[3]:

    df_cluster['cluster'] = kmeans_model.predict(preprocessor_cluster.transform(df_cluster[xvar_cluster]))

    return df_cluster
