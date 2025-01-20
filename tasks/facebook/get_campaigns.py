from core.get_data import download_data, process_responses
from sources import campaigns

def download_campaigns(token, params, control_group, kind):

    meta = download_data(token, campaigns, kind, control_group=control_group, params=params)
    return meta

def process_campaigns(meta):
    control_group = meta["control_group"]
    # Process json responses as csv
    meta = process_responses(campaigns, control_group=control_group, params_cols=["country", "company"])
    return meta


# if __name__ == '__main__':
#     account_id = 'BNF_MX'
#     kind = 'new'
#     meta = download_campaigns(token, params, control_group, kind)
#     process_campaigns(meta)
