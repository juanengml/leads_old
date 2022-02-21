

def write_recommendations(recommendations):
    pass


def prepare_lead(leads):
    return leads


def get_brokers():
    """Get brokers on duty and with available capacity"""
    output = [1, 2, 3]
    return output


def filter_brokers(brokers):
    """Filter by UF, capacity and Fair Indices"""
    return brokers


def update_broker(broker):
    """Update broker capacity and Fair Indices for internal control"""
    pass


def process_lead(lead, brokers):
    output = ([1, 2, 3], [4.9, 3.0, 1.2])
    return output


def run_motor(leads):
    leads = prepare_lead(leads)
    brokers = get_brokers()
    recommendation_data = []
    for lead in leads:
        filtered_brokers = filter_brokers(brokers)
        recommended_brokers, recommended_scores = process_lead(lead, filtered_brokers)
        update_broker(recommended_brokers)

        recommendation_data.append((recommended_brokers,
                                    recommended_scores))

    write_recommendations(recommendation_data)
