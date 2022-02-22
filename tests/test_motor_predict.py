import json

import pandas as pd
import pytest

from motor_predict_new import prepare_lead, get_brokers, filter_brokers, \
    process_lead


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
def internal_brokers():
    return ["3416837525"]


@pytest.fixture
def brokers():
    f = open('sample_brokers.json', 'r')
    return json.loads(f.read())


def test_prepare_lead(leads_data):
    """Test leads data preparation."""
    output = prepare_lead(leads_data)
    assert isinstance(output, pd.DataFrame), "Wrong leads type"
    assert len(output) == 6, "Incorrect number of leads in output"


def test_prepare_lead_errors(leads_data_error):
    """Check AttributeError raised when a specific column is not found."""
    with pytest.raises(AttributeError):
        prepare_lead(leads_data_error)


def test_get_brokers(shifts, internal_brokers):
    """Test brokers ready for work"""
    output = get_brokers(shifts, internal_brokers)
    assert isinstance(output, list), "Wrong brokers type"
    assert len(output) > 0, "Incorrect number of brokers"


def test_filter_brokers(prepared_lead_data, brokers):
    output = filter_brokers(prepared_lead_data, brokers)

    assert isinstance(output, list), "Wrong brokers type"
    assert len(output) == len(brokers), "Filtered list must be equal " \
                                        "original list"


def test_process_lead(prepared_lead_data, brokers):
    output = process_lead(prepared_lead_data, brokers)



