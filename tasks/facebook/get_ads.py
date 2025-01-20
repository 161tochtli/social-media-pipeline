from tqdm import tqdm
from core.get_data import download_data, process_responses
from sources import campaign_ads

def download_campaign_ads(token, active_campaigns, params, control_group, kind):
    md = None
    for i in tqdm(active_campaigns):
        params.update({'campaign_id': i})
        md = download_data(token, campaign_ads, kind, params=params, control_group=control_group)
    return md


def process_campaign_ads(meta):
    control_group = meta["control_group"]
    # Process json responses as csv
    def transform_response(response):
        # Desanidamos el creative id
        for item in response["data"]:
            item["creative_id"] = item.get("creative", {}).get("id")
        return response

    meta_cols = ["id","name","account_id","campaign_id","adset_id","creative_id","created_time","effective_status","source_ad_id","status","updated_time","recommendations"]

    md = process_responses(campaign_ads, control_group=control_group,
                      params_cols=["campaign_id", "country", "company"], params_prefix=None,
                      transform_response=transform_response,
                      record_path=["adcreatives","data"],
                      record_prefix="creative.",
                      meta=meta_cols,
                      meta_prefix='ad.')
    return md


