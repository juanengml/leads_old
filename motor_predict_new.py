import json
import requests
import datetime
from datetime import timedelta

import numpy as np
import unidecode
import pandas as pd

from config import HEADERS, URL_PLANTAO

INTERNAL_BROKER_FILENAME = 'historico_bi/internal_brokers.xlsx'


def request_broker_shifts():
    """Get shifts that are happening right now.

    First we seek for all the shifts for today, secondly we filter by
    shifts happening right now

    Returns:
        list: List of dict representing shifts
    """
    start = str(datetime.today().replace(hour=0, minute=0, second=0))
    end = str(datetime.today().replace(hour=23, minute=59, second=59))
    timestamp = datetime.timestamp(datetime.now())

    body = {'DataHoraInicio': start, 'DataHoraFim': end}
    response = requests.request('POST', URL_PLANTAO, headers=HEADERS,
                                data=body)
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

    brokers = list(filter(lambda x: x['cpf'] in internal_brokers, brokers))
    return brokers


def get_internal_brokers(filepath):
    return pd.read_excel(filepath).CPF.unique.tolist()


def get_shifts():
    timestamp = datetime.timestamp(datetime.now())

    shifts = request_broker_shifts()
    shifts = filter(lambda x: x['DataHoraFim'] > timestamp, shifts)
    shifts = filter(lambda x: x['status'] == 'Em andamento', shifts)
    return shifts


def filter_brokers(lead, brokers):
    """Filter by UF, capacity and Fair Indices

    Args:
        lead (dict): Leads informations
        brokers (list): Broker list (each element is a dict)

    Returns:
        list: Broker list (each element is a dict)

    """
    lead_uf = lead['UF']
    brokers = list(filter(lambda x: x['uf'] == lead_uf, brokers))
    brokers = list(filter(lambda x: get_matches_quantity(x['cpf']) <
                                    get_capacity_broker(x['cpf']),
                          brokers))
    return brokers


def update_broker(broker_cpf):
    """Update broker capacity and Fair Indices (if necessary) for internal control"""
    # TODO Do it by database operations
    pass


def get_capacity_broker(broker_cpf):
    """Query the broker's capacity registered in the internal database"""
    # TODO Do it by database operations
    return 30


def get_matches_quantity(broker_cpf, days=30):
    """Query matches quantity for a specific broker within 30 days"""
    date = datetime.datetime.today() - timedelta(days=days)
    # TODO Do it by database operations
    return 0


def insert_lead(lead):
    """Insert lead in the database"""
    # TODO Do it by database operations
    return True


def insert_match(lead, broker, score):
    """Insert lead in the database"""
    # TODO Do it by database operations
    return True


def process_lead(lead, brokers):
    output = ([1, 2, 3], [4.9, 3.0, 1.2])
    return output


def run_motor(leads):
    leads = prepare_lead(leads)

    shifts = request_broker_shifts()
    brokers = get_brokers(shifts, get_internal_brokers(INTERNAL_BROKER_FILENAME))
    update_broker(brokers)

    recommendation_data = []
    for idx, lead in leads.iterrows():
        filtered_brokers = filter_brokers(brokers.copy())

        if len(filtered_brokers) == 0:
            # Random Recommendation
            continue

        # TODO Until now we only use the best recommendation
        recommended_brokers, recommended_scores = process_lead(lead, filtered_brokers)

        insert_lead(lead)
        insert_match(lead, recommended_brokers[0], recommended_scores[0])
        recommendation_data.append((recommended_brokers,
                                    recommended_scores))
