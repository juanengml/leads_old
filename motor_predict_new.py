import json
import requests
import random
import datetime
from datetime import timedelta

import numpy as np
import unidecode
import pandas as pd

import corretores_lib
import prepara_dados
import aux_function
from config import HEADERS, URL_PLANTAO
from artefatos import features, preprocessor, preprocessor_cluster, \
    xgboost_loaded_model, categoria_treino, features_cluster, matriz

INTERNAL_BROKER_FILENAME = 'historico_bi/internal_brokers.xlsx'


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
    body = get_request_body(datetime.today, datetime.today)
    response = requests.request('POST', URL_PLANTAO, headers=HEADERS,
                                data=body)

    timestamp = datetime.timestamp(datetime.now())

    shifts = json.loads(response.content)
    shifts = filter(lambda x: x['DataHoraFim'] > timestamp, shifts)
    shifts = filter(lambda x: x['status'] == 'Em andamento', shifts)
    return shifts


def prepare_lead(leads):
    """ Prepares and validates lead's interest variables

    Args:
        leads (dict): Leads data

    Returns:
        pd.DataFrame: Leads information casted to a pd.DataFrame
    """
    leads = pd.DataFrame(leads)

    if 'FL_CLIENTE_ATIVO' not in leads.columns:
        leads['FL_CLIENTE_ATIVO'] = False

    cols = ['MIDIA_VEICULO', 'MIDIA_FORMATO', 'PROFISSAO',
            'ENTIDADE', 'MUNICIPIO', 'GRUPO_ORIGEM', 'NM_ORIGEM']

    for c in cols:
        if c not in leads.columns:
            raise AttributeError(f'Coluna {c} não encontrada.')

    leads.rename(columns={'MIDIA_VEICULO': 'ORIGEM_DE_MIDIA_VEICULO',
                          'MIDIA_FORMATO': 'ORIGEM_DE_MIDIA_FORMATO',
                          'PROFISSAO': 'ds_profissao',
                          'ENTIDADE': 'EntidadeLead',
                          'MUNICIPIO': 'CIDADE',
                          'GRUPO_ORIGEM': 'GRUPOORIGEM'}, inplace=True)
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

    brokers = list(filter(lambda x: x['cpf'] in internal_brokers,
                          brokers))
    return brokers


def get_internal_brokers(filepath):
    """

    Args:
        filepath:

    Returns:

    """
    return pd.read_excel(filepath).CPF.unique.tolist()


def get_shifts():
    """

    Returns:

    """
    timestamp = datetime.timestamp(datetime.now())

    shifts = request_broker_shifts()
    shifts = filter(lambda x: x['DataHoraFim'] > timestamp, shifts)
    shifts = filter(lambda x: x['status'] == 'Em andamento', shifts)
    return shifts


def filter_brokers(lead, brokers_on_duty):
    """Filter by UF, capacity and Fair Indices

    Args:
        lead (dict): Leads informations
        brokers_on_duty (list): Broker list (each element is a dict)

    Returns:
        list: Broker list (each element is a dict)

    """
    lead_uf = lead['UF']
    brokers_on_duty = select_broker(brokers_on_duty, lead_uf)
    brokers_on_duty = list(filter(lambda x: get_matches_quantity(x) <
                                            get_capacity_broker(x),
                                  brokers_on_duty))

    brokers_matches = list(map(lambda x: get_matches_quantity(x),
                               brokers_on_duty))

    if len(brokers_on_duty) == 0 or sum(brokers_matches) == 0:
        return brokers_on_duty

    brokers_fair = list(map(lambda x: x / sum(brokers_matches),
                            brokers_matches))
    max_proportion = 1 / len(brokers_on_duty)
    brokers_fair = list(filter(lambda x: x[1] < max_proportion,
                               zip(brokers_on_duty, brokers_fair)))
    brokers_fair = list(map(lambda x: x[0], brokers_fair))
    return brokers_fair


def select_broker(broker_cpf, uf):
    """Select broker data by cpf and uf"""
    # TODO Do it by database operations
    return broker_cpf


def update_broker(broker_cpf):
    """Update broker capacity and Fair Indices (if necessary) for internal
    control"""
    # TODO Do it by database operations
    return True


def get_capacity_broker(broker_cpf):
    """Query the broker's capacity registered in the internal database"""
    # TODO Do it by database operations
    return 30


def get_matches_quantity(broker_cpf, days=0):
    """Query matches quantity for a specific broker in the day"""
    date = datetime.datetime.today().replace(hour=0, minute=0, second=0)
    date = date - timedelta(days=days)
    # TODO Do it by database operations
    return 1


def insert_lead(lead):
    """Insert lead in the database"""
    # TODO Do it by database operations
    return True


def insert_match(lead, broker, score):
    """Insert lead in the database"""
    # TODO Do it by database operations
    return True


def get_matriz_justa(filtered_matriz, justice_degree=0.1):
    """
    ?
    Args:
        filtered_matriz:
        justice_degree:

    Returns:

    """
    matriz_justa = pd.DataFrame()
    for lead in filtered_matriz.index:
        cluster_max = filtered_matriz.loc[lead, :].sort_values(
            ascending=False).max()
        cluster_min = filtered_matriz.loc[lead, :].sort_values(
            ascending=False).min()
        cluster_range = np.arange(cluster_min, cluster_max, justice_degree)
        matriz_justa = filtered_matriz.estimativa.loc[lead, :].apply(
            lambda y: cluster_range[abs(cluster_range - y).argmin()])

    matriz_justa = pd.DataFrame(matriz_justa).T
    matriz_justa = matriz_justa.reset_index()
    matriz_justa.rename(columns={'index': 'Leads'}, inplace=True)

    matriz_justa.fillna(1, inplace=True)
    return matriz_justa.copy()


def lead_recommendation(lead, filtered_brokers):
    """

    Args:
        lead:
        filtered_brokers:

    Returns:

    """
    # lead = pd.Series(lead).to_frame().T
    # x = prepara_dados.data_cluster_prep(lead, features_cluster)
    # filtered_matriz = matriz[matriz['corretor_id'].isin(
    #     filtered_brokers)]
    # filtered_matriz.sort_values(by='corretor_id', inplace=True)
    # filtered_matriz = pd.merge(filtered_matriz,
    #                            lead[['ID_LEAD', 'cluster']],
    #                            how='left', on='cluster').dropna()
    # filtered_matriz = filtered_matriz[['ID_LEAD',
    #                                    'corretor_id',
    #                                    'estimativa']].pivot(
    #     index='ID_LEAD', columns='corretor_id')
    #
    # # TODO Recommend new brokers to a redistributed lead
    #
    # matriz_justa = get_matriz_justa(filtered_matriz)
    # recommendation = corretores_lib.recommender(matriz_justa, x,
    #                                             redistribuido=False)
    # if len(recommendation) == 0:
    #     return None, None
    #
    # match_broker = recommendation['CPF_CORRETOR']
    # match_score = recommendation['RATING']

    match_broker = "3416837525"
    match_score = 4.5
    return match_broker, match_score


def lead_score(lead):
    """

    Args:
        lead:

    Returns:

    """
    # cat = features['cat']
    # num = features['num']
    # xvar = features['xvar']
    #
    # lead = pd.Series(lead).to_frame().T
    # _, lead = prepara_dados.data_clean(lead, cat, num)
    # # TODO raise error if category does not exists
    # lead_x = preprocessor.transform(lead[xvar])
    # lead_x = pd.DataFrame(lead_x)
    # lead_x.columns = num + cat
    # proba = xgboost_loaded_model.predict_proba(lead_x[xvar].values)
    # label = np.argmax(proba, axis=1)
    # proba = proba[:, 1]
    proba = 0.8
    label = 1
    return label, proba


def process_lead(lead, filtered_brokers):
    """

    Args:
        lead:
        filtered_brokers:

    Returns:

    """
    lead_labels, lead_proba = lead_score(lead)
    lead['score'] = lead_proba
    lead['flag_previsao'] = lead_labels
    lead['Score_xgboost_bin'] = aux_function.Score_bins([lead['score']])
    lead['Score_xgboost_bin'] = lead['Score_xgboost_bin'].tolist()[0]
    lead['age_bins'] = aux_function.Age_bins([lead['DATA_NASCIMENTO']])
    lead['age_bins'] = lead['age_bins'].tolist()[0]

    return lead_recommendation(lead, filtered_brokers)


def run_motor(leads):
    """

    Args:
        leads:

    Returns:

    """
    leads = prepare_lead(leads)

    shifts = request_broker_shifts()
    brokers = get_brokers(shifts, get_internal_brokers(INTERNAL_BROKER_FILENAME))
    update_broker(brokers)

    recommendation_output = {}
    for idx, lead in leads.iterrows():
        lead = lead.to_dict()
        filtered_brokers = filter_brokers(lead, brokers.copy())

        recommended_score = 0.0

        if len(filtered_brokers) == 0:
            # TODO What to
            recommended_broker = random.choice(brokers)
        else:
            recommended_broker, recommended_score = process_lead(
                lead, filtered_brokers)

            if not recommended_broker:
                recommended_broker = random.choice(filtered_brokers)

        insert_lead(lead)
        insert_match(lead, recommended_broker, recommended_score)
        recommendation_output[idx] = {"ID_LEAD": lead['cpf'],
                                      "CPF_CORRETOR": recommended_broker,
                                      "RATING": recommended_score}


