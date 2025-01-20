from dotenv import load_dotenv, dotenv_values
from core.get_data import download_data, process_responses
from sources import feed


# Cargar variables de entorno
load_dotenv(".secrets/meta_tokens.txt")
account_ids =  dotenv_values(".secrets/account_ids.txt")
source = feed


def download_feed_posts(token, params, control_group, kind):
    meta = download_data(token, source, kind, control_group=control_group, params=params)
    return meta


def process_feed_posts(meta):
    control_group = meta["control_group"]

    def transform_feed(response):
        data = response.get("data", [])
        for record in data:
            actions = record.pop("actions", [])
            record.update({f"actions.{item['name']}": True for item in actions if 'name' in item})
        return response

    # Process json responses as csv
    meta = process_responses(source, control_group=control_group, params_cols=["page_id", "country", "company"], transform_response=transform_feed)
    return meta

#
# if __name__ == '__main__':
#     # pages_ids = {"BNF_MX": "1614029768874636"}# , "BV_MX": "103667945225282", "BNF_CL": "104433997637460", "BNF_CO": "114787541595729"}
#     page_id = 'BNF_MX'
#     kind = 'new'
#     meta = download_feed_posts(account_id, kind)
#     process_feed_posts(meta)

