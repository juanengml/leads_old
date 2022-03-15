import os
import json
import requests
import random
from datetime import timedelta, datetime

import numpy as np
import unidecode
import pandas as pd

import historico
import corretores_lib
import prepara_dados
import aux_function
import db.operations as db_operations
from log import Logger
from config import HEADERS, URL_PLANTAO, main_path, URL_TEAMS
from artefatos import features, preprocessor, xgboost_loaded_model, \
    features_cluster, matriz

log = Logger(os.path.join(main_path, 'leads'))
INTERNAL_BROKER_FILENAME = 'historico_bi/consultores_canal_interno.xlsx'


def get_request_body(start, end):
    """
    Args:
        start:
        end:

    Returns:

    """
    start = str(start.replace(hour=0, minute=0, second=0))
    end = str(end.replace(hour=23, minute=59, second=59))
    body = {'DataHoraInicio': start, 'DataHoraFim': end}
    return body


def request_broker_shifts():
    """Get shifts that are happening right now.

    First we seek for all the shifts for today, secondly we filter by
    shifts happening right now

    Returns:
        list: List of dict representing shifts
    """
    body = get_request_body(datetime.today(), datetime.today())
    response = requests.request('POST', URL_PLANTAO, headers=HEADERS,
                                data=body)

    timestamp = datetime.timestamp(datetime.now())

    shifts = json.loads(response.content)
    shifts = filter(lambda x: x['DataHoraFim'] > timestamp, shifts)
    shifts = filter(lambda x: x['status'] == 'Em andamento', shifts)
    return shifts


def prepare_lead(leads):
    """ Prepare and validates lead's interest variables

    Args:
        leads (dict): Leads data

    Returns:
        pd.DataFrame: Leads information casted to a pd.DataFrame
    """
    leads = pd.DataFrame(data=leads)

    if 'FL_CLIENTE_ATIVO' not in leads.columns:
        leads['FL_CLIENTE_ATIVO'] = False

    cols = ['MIDIA_VEICULO', 'MIDIA_FORMATO', 'PROFISSAO',
            'ENTIDADE', 'MUNICIPIO', 'GRUPO_ORIGEM', 'NM_ORIGEM']

    for c in cols:
        if c not in leads.columns:
            leads[c] = np.nan

    leads.rename(columns={'MIDIA_VEICULO': 'ORIGEM_DE_MIDIA_VEICULO',
                          'MIDIA_FORMATO': 'ORIGEM_DE_MIDIA_FORMATO',
                          'PROFISSAO': 'ds_profissao',
                          'ENTIDADE': 'EntidadeLead',
                          'MUNICIPIO': 'CIDADE',
                          'GRUPO_ORIGEM': 'GRUPOORIGEM'}, inplace=True)

    leads['ORIGEM_DE_MIDIA_VEICULO'] = leads['ORIGEM_DE_MIDIA_VEICULO'].astype(str)
    leads['ORIGEM_DE_MIDIA_FORMATO'] = leads['ORIGEM_DE_MIDIA_FORMATO'].astype(str)
    leads['EntidadeLead'] = leads['EntidadeLead'].astype(str)

    leads['CIDADE'] = leads.CIDADE.astype(str).apply(
        lambda x: unidecode.unidecode(x.lower()))
    leads['GRUPOORIGEM'] = leads.GRUPOORIGEM.astype(str).apply(
        lambda x: unidecode.unidecode(x.lower()))
    leads['NM_ORIGEM'] = leads.apply(lambda df: df.GRUPOORIGEM if (
            df.NM_ORIGEM == 'nan' or df.NM_ORIGEM == np.nan
    ) else df.NM_ORIGEM, axis=1)

    # TODO Refactor data_prep
    leads = prepara_dados.data_prep(leads)
    return leads


def get_brokers(shifts, internal_brokers):
    """Get brokers on duty and with available capacity"""
    # TODO Validate types
    brokers = [item for sublist in shifts for item in sublist['Corretores']]

    brokers = list(filter(lambda x: not x['pausado'], brokers))
    brokers = list(filter(lambda x: x['canal'] == 'INTERNO',
                          brokers))

    brokers = list(filter(lambda x: x['leadsDistribuidos'] <
                                    x['quantidadeLeads'], brokers))

    # brokers = list(filter(lambda x: x['cpf'] in internal_brokers,
    #                       brokers))
    return brokers


def get_internal_brokers(filepath):
    """

    Args:
        filepath:

    Returns:

    """
    return pd.read_excel(filepath).CPF.unique().tolist()


def get_shifts():
    """

    Returns:

    """
    timestamp = datetime.timestamp(datetime.now())

    shifts = request_broker_shifts()
    shifts = filter(lambda x: x['DataHoraFim'] > timestamp, shifts)
    shifts = filter(lambda x: x['status'] == 'Em andamento', shifts)
    return shifts


def be_fair(brokers, working_days, matches, min_days=5):
    brokers_fair = list(filter(lambda x: x[1] >= min_days,
                               zip(brokers, working_days)))

    MAX_PROPORTION = 1 / len(brokers_fair)
    brokers_fair = list(map(lambda x: x[0], brokers_fair))

    # Requirements to use fair indices
    if len(brokers_fair) == 0:
        return brokers

    proportion = list(map(lambda x: x / sum(matches),
                            matches))
    proportion = list(filter(lambda x: x[1] <= MAX_PROPORTION,
                               zip(brokers, proportion)))
    return list(map(lambda x: x[0], proportion))


def filter_brokers(lead, brokers_on_duty, working_days=5):
    """Filter by UF, capacity and Fair Indices

    Args:
        working_days (int): How many working days a broker needs to use
         fair indice
        lead (dict): Leads informations
        brokers_on_duty (list): Broker list (each element is a dict)

    Returns:
        list: Broker list (each element is a dict)

    """
    lead_uf = lead['UF']
    brokers_on_duty = select_brokers(brokers_on_duty, lead_uf)
    brokers_on_duty = list(filter(lambda x: get_matches_quantity(x) <
                                            get_capacity_broker(x),
                                  brokers_on_duty))

    brokers_matches = list(map(lambda x: get_matches_quantity(x),
                               brokers_on_duty))

    if len(brokers_on_duty) == 0 or sum(brokers_matches) == 0:
        return brokers_on_duty

    brokers_working_days = list(map(lambda x: get_working_days(x),
                                    brokers_on_duty))

    return be_fair(brokers_on_duty, brokers_working_days,
                      brokers_matches, working_days)


def select_brokers(brokers_cpf, uf):
    """Select broker data by cpf and uf"""
    brokers_cpf = tuple(map(int, brokers_cpf))
    sql = f'SELECT cpf FROM leads_broker WHERE uf=? AND cpf IN {brokers_cpf}'
    brokers_cpf = db_operations.do_select(sql, [uf])
    brokers_cpf = [b[0] for b in brokers_cpf]
    return brokers_cpf


def update_broker(brokers):
    """Update broker capacity and Fair Indices (if necessary) for
     internal control"""
    sql = 'INSERT INTO leads_broker(cpf, capacity, uf)' \
          'VALUES (?, ?, ?)'
    brokers = list(map(lambda x: (x['cpf'],
                                  x['quantidadeLeads'],
                                  x['uf']), brokers))

    list(map(lambda x: db_operations.do_insert(sql, x), brokers))
    sql_update = 'UPDATE leads_broker SET capacity=? where cpf=?'

    result = list(map(lambda x: db_operations.do_update(sql_update,
                                                        [x[0], x[1]]),
                      brokers))
    return all(result)


def get_working_days(broker_cpf):
    """Query how many days the broker has data in the database"""
    sql = f'SELECT COUNT(*) FROM leads_match WHERE cpf_broker = ?'
    return db_operations.do_select(sql, [broker_cpf])[0][0]


def get_capacity_broker(broker_cpf):
    """Query the broker's capacity registered in the internal database"""
    sql = f'SELECT capacity FROM leads_broker WHERE cpf = ?'
    return db_operations.do_select(sql, [broker_cpf])[0][0]


def get_matches_quantity(broker_cpf, days=0):
    """Query matches quantity for a specific broker in the day"""
    date = datetime.today().replace(hour=0, minute=0, second=0)
    date = date - timedelta(days=days)
    sql = f'SELECT COUNT(*) FROM leads_match WHERE cpf_broker = ? AND date > ?'
    return db_operations.do_select(sql, [broker_cpf, date])[0][0]


def insert_lead(lead):
    """Insert lead in the database"""
    sql = 'INSERT INTO leads_lead(cpf) VALUES (?)'
    return db_operations.do_insert(sql, [lead])


def insert_match(lead, broker, score, method):
    """Insert lead in the database"""
    sql = 'INSERT INTO leads_match(date, score, cpf_broker, cpf_lead, ' \
          'method) VALUES (datetime("now"), ?, ?, ?, ?)'
    return db_operations.do_insert(sql, [score, broker, lead, method])


def get_justice_matrix(filtered_matrix, justice_degree=0.1):
    """
    ?
    Args:
        filtered_matrix:
        justice_degree:

    Returns:

    """
    justice_matrix = pd.DataFrame()
    for lead in filtered_matrix.index:
        cluster_max = filtered_matrix.loc[lead, :].sort_values(
            ascending=False).max()
        cluster_min = filtered_matrix.loc[lead, :].sort_values(
            ascending=False).min()
        cluster_range = np.arange(cluster_min, cluster_max, justice_degree)
        if len(cluster_range) == 0:
            cluster_range = np.array([cluster_min])
        justice_matrix = filtered_matrix.estimativa.loc[lead, :].apply(
            lambda y: cluster_range[abs(cluster_range - y).argmin()])

    justice_matrix = pd.DataFrame(justice_matrix).T
    justice_matrix = justice_matrix.reset_index()
    justice_matrix.rename(columns={'index': 'Leads'}, inplace=True)

    justice_matrix.fillna(1, inplace=True)
    return justice_matrix.copy()


def lead_recommendation(lead, filtered_brokers):
    """

    Args:
        lead:
        filtered_brokers:

    Returns:

    """
    lead = prepara_dados.data_cluster_prep(lead, features_cluster)
    # TODO CPF as float?
    filtered_brokers_float = list(map(float, filtered_brokers))
    filtered_matrix = matriz[matriz['corretor_id'].isin(
        filtered_brokers_float)].copy()
    filtered_matrix.sort_values(by='corretor_id', inplace=True)
    filtered_matrix = pd.merge(filtered_matrix,
                               lead[['ID_LEAD', 'cluster']],
                               how='left', on='cluster').dropna()
    filtered_matrix = filtered_matrix[['ID_LEAD',
                                       'corretor_id',
                                       'estimativa']].pivot(
        index='ID_LEAD', columns='corretor_id')

    # TODO Recommend new brokers to a redistributed lead

    justice_matrix = get_justice_matrix(filtered_matrix)
    recommendation = corretores_lib.recommender(justice_matrix, lead,
                                                redistribuido=False)
    if len(recommendation) == 0:
        return None, None

    match_broker = recommendation['CPF_CORRETOR'].values[0]
    # match_broker = str(int(match_broker))
    # if len(match_broker) < 11:
    #     match_broker = f"0{int(match_broker)}"
    match_score = recommendation['RATING'].values[0]
    return match_broker, match_score


def lead_score(lead):
    """

    Args:
        lead:

    Returns:

    """
    cat = features['cat']
    num = features['num']
    xvar = features['xvar']

    _, lead = prepara_dados.data_clean(lead, cat, num)
    # TODO What happens if category does not exists?
    lead_x = preprocessor.transform(lead[xvar])
    lead_x = pd.DataFrame(lead_x)
    lead_x.columns = num + cat
    proba = xgboost_loaded_model.predict_proba(lead_x[xvar].values)
    label = np.argmax(proba, axis=1)[0]
    proba = proba[:, 1][0]
    return label, proba


def process_lead(lead, filtered_brokers):
    """

    Args:
        lead:
        filtered_brokers:

    Returns:

    """
    lead = pd.Series(lead).to_frame().T
    lead_labels, lead_proba = lead_score(lead)
    lead['score'] = lead_proba
    lead['flag_previsao'] = lead_labels
    lead['Score_xgboost_bin'] = aux_function.Score_bins(lead['score'])
    lead['age_bins'] = aux_function.Age_bins(lead['DATA_NASCIMENTO'])

    return lead_recommendation(lead, filtered_brokers), lead_proba


def random_recommendation(lead_cpf, brokers):
    """
    Recommend a random broker to a specific lead and returns a standard
    output message
    Args:
        lead_cpf (int): Lead's cpf
        brokers (list): List of brokers on duty

    Returns (dict): Standard output message
    """
    recommended_broker = random.choice(brokers)
    insert_lead(lead_cpf)
    insert_match(lead_cpf, recommended_broker, 0.0, "RANDOM")
    output = {"PF_CORRETOR": recommended_broker,
              "CPF_CORRETOR_2": recommended_broker,
              "CPF_CORRETOR_3": recommended_broker,
              "ID_LEAD": lead_cpf,
              "RATING": 0.0, "RATING_2": 0.0,
              "RATING_3": 0.0, "SCORE": 0.0,
              }
    return output


def run_motor(leads, shifts=None, internal_brokers=None):
    """

    Args:
        internal_brokers:
        shifts:
        leads:

    Returns:

    """
    log.info('Iniciando recomendação de leads')
    leads = prepare_lead(leads)

    if not shifts:
        shifts = get_shifts()
        internal_brokers = get_internal_brokers(INTERNAL_BROKER_FILENAME)

    brokers = get_brokers(shifts, internal_brokers)
    log.info(f'{len(brokers)} corretores estão de plantão')

    update_broker(brokers)
    brokers = list(map(lambda x: x['cpf'], brokers))

    log.info(f'Processando {len(leads)} leads')
    recommendation_output = {}
    for idx, lead in leads.iterrows():
        idx = str(idx)
        # TODO Map internal errors so that a try catch here can be removed
        try:
            lead = lead.to_dict()
            lead_cpf =float(lead['ID_LEAD'])

            log.info(f'Lead {lead_cpf}, iniciando recomendação')
            filtered_brokers = filter_brokers(lead, brokers.copy())
            log.info(f'{len(filtered_brokers)} possíveis candidatos'
                     f' encontrados')
            if len(filtered_brokers) < 1:
                log.info('Recomendando nada')
                recommendation_output[idx] = {}
                continue
            if len(filtered_brokers) == 1:
                # TODO What to do if no broker on duty with the same uf,
                #  available capacity and fair indice is found?
                log.info('Recomendando aleatório')
                recommendation_output[idx] = random_recommendation(lead_cpf, filtered_brokers)
                continue
            
            (recommended_broker, recommended_score), score = process_lead(
                lead, filtered_brokers)
            log.info(f'Resultado recomendação: {recommended_broker}')

            if not recommended_broker:
                log.info(f'Nenhum corretor pode atender o lead. '
                         f'Recomendando aleatório')
                recommendation_output[idx] = random_recommendation(lead_cpf, brokers)
                continue
            insert_lead(lead_cpf)
            insert_match(lead_cpf, recommended_broker, recommended_score,
                         'RECOMMENDATION')
            recommendation_output[idx] = {"CPF_CORRETOR": recommended_broker,
                                          "CPF_CORRETOR_2": recommended_broker,
                                          "CPF_CORRETOR_3": recommended_broker,
                                          "ID_LEAD": lead_cpf,
                                          "RATING": recommended_score,
                                          "RATING_2": recommended_score,
                                          "RATING_3": recommended_score,
                                          "SCORE": float(score)}
        except BaseException as ex:
            log.error(f'Erro capturado: {ex}. Recomendando aleatório')
            recommendation_output[idx] = random_recommendation(lead_cpf, brokers)
    try:
        historico.Teams_message(URL_TEAMS, recommendation_output)
    except:
        pass
    return recommendation_output
