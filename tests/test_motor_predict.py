import json
import datetime

import numpy as np
import pandas as pd
import pytest

from motor_predict_new import prepare_lead, get_brokers, filter_brokers, \
    process_lead, lead_score, lead_recommendation, get_request_body


@pytest.fixture
def leads_data():
    f = open('sample_leads.json', 'r')
    return json.loads(f.read())


@pytest.fixture
def prepared_lead_data():
    f = open('sample_prepared_lead.json', 'r')
    return json.loads(f.read())


@pytest.fixture
def leads_data_error():
    f = open('sample_leads_error.json', 'r')
    return json.loads(f.read())


@pytest.fixture
def shifts():
    f = open('sample_shifts.json', 'r')
    return json.loads(f.read())


@pytest.fixture
def brokers():
    return ["41266296883",
            "20542776812"]


@pytest.fixture
def scored_lead():
    f = open('sample_scored_lead.json', 'r')
    return json.loads(f.read())


@pytest.fixture
def request_body():
    return {"DataHoraInicio": "2022-02-22 00:00:00",
            "DataHoraFim": "2022-02-22 23:59:59"}


def test_get_request_body(request_body):
    """Test body of the request for broker API."""
    date = datetime.datetime.strptime("22/02/2022",
                                      "%d/%m/%Y")
    expected = request_body
    output = get_request_body(date, date)
    assert expected == output, "Incorrect datetime format"


def test_prepare_lead(leads_data):
    """Test leads data preparation."""
    output = prepare_lead(leads_data)
    assert isinstance(output, pd.DataFrame), "Wrong leads type"
    assert len(output) == 6, "Incorrect number of leads in output"


def test_prepare_lead_errors(leads_data_error):
    """Check AttributeError raised when a specific column is not found."""
    with pytest.raises(AttributeError):
        prepare_lead(leads_data_error)


def test_get_brokers(shifts, brokers):
    """Test brokers ready for work."""
    output = get_brokers(shifts, brokers)
    assert isinstance(output, list), "Wrong brokers type"
    assert len(output) > 0, "Incorrect number of brokers"


def test_filter_brokers(prepared_lead_data, brokers):
    """Test if broker that have already reached fair indice."""
    output = filter_brokers(prepared_lead_data, brokers)

    assert isinstance(output, list), "Wrong brokers type"
    assert len(output) == len(brokers), "Filtered list must be equal " \
                                        "original list"


def test_lead_score(prepared_lead_data):
    """Test lead score prediction."""
    output = lead_score(prepared_lead_data)
    assert isinstance(output, tuple), "Wrong output type"
    assert isinstance(output[0], np.integer), "Wrong label type"
    assert isinstance(output[1], np.floating), "Wrong proba type"


def test_lead_recommendation(scored_lead, brokers):
    """Test lead-broker recommendation."""
    output = lead_recommendation(scored_lead, brokers)
    assert isinstance(output, tuple), "Wrong output type"
    assert isinstance(output[0], str), "Wrong label type"
    assert isinstance(output[1], np.floating), "Wrong proba type"


def test_process_lead(prepared_lead_data, brokers):
    """Test processing steps for the lead score prediction and
    recommendation."""
    output = process_lead(prepared_lead_data, brokers)
    assert isinstance(output, tuple), "Wrong output type"
    assert isinstance(output[0], str), "Wrong label type"
    assert isinstance(output[1], np.floating), "Wrong proba type"


def test_integration_process_lead(leads_data, brokers):
    """Test prepare_leads(), filter_brokers() and process_lead() workflow."""
    prepared_leads = prepare_lead(leads_data)
    prepared_lead = prepared_leads.iloc[0, :]
    filtered_brokers = filter_brokers(prepared_lead, brokers)
    recommended_broker, recommended_score = process_lead(
        prepared_lead, filtered_brokers)
    assert isinstance(recommended_broker, str), "Wrong label type"
    assert isinstance(recommended_score, np.floating), "Wrong proba type"






