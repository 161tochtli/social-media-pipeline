from dotenv import load_dotenv, dotenv_values
import datetime as dt
import os
from core.get_data import download_data, process_responses
from sources import ig_media

load_dotenv(".secrets/meta_tokens.txt")
account_ids =  dotenv_values(".secrets/account_ids.txt")

def download_ig_media(token, params, control_group, kind='new'):
    meta = download_data(token, ig_media, kind, params=params, control_group=control_group)
    return meta

def process_ig_media(meta):
    control_group = meta["control_group"]
    # Process json responses as csv

    # def transform_comments_ig(response):
    #     data = response.get("data", [])
    #     for record in data:
    #         record.pop("message_tags", None)
    #         record.pop("reactions", None)
    #     return response

    meta = process_responses(ig_media, control_group=control_group, params_cols=["brand", "country", "platform","user_id"])#, transform_response=transform_comments_ig)

    return meta


# if __name__ == '__main__':
#     start_time = None#"2015-01-01T00:00:00-0600"
#     end_time = None#"2024-12-31T23:59:59-0600"
#     account_name = "IG_BNF_MX"
#     token_type = "PAGE"
#
#     token = os.environ.get(f"{account_name}_{token_type}_TOKEN")
#     if not token:
#         raise ValueError(f"Token for {account_name} with type {token_type} not found.")
#
#     print(f"Starting process for account: {account_name}")
#     start_time = f"{start_time.split('T')[0]}" if start_time else ""
#     end_time = f"{end_time.split('T')[0]}" if end_time else ""
#     timestamp = f"{start_time}{end_time}"
#     if not timestamp:
#         timestamp = dt.datetime.now().strftime("%Y%m%d")
#     control_group = f"{account_name}_{timestamp}"
#     account_id = account_ids[account_name]
#     platform, brand, country = account_name.split("_")
#
#     params = {"brand": brand, "country": country, "platform":platform, "account_id":account_id}
#
#     md = download_ig_media(token,params,control_group=control_group,kind='new')
#     process_ig_media(md)
