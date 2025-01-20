from tqdm import tqdm
from core.get_data import download_data, process_responses
from sources import comments

source = comments

def download_comments(token, object_ids, params, control_group, kind='new'):
    # run download_data task for each effective_object_story_id as param
    print(f"Total posts: {len(object_ids)}")
    meta = None
    for i in tqdm(object_ids):
        params.update({"object_id": i})
        meta = download_data(token, source, kind, params=params, control_group=control_group)
    return meta


def process_comments(meta):
    control_group = meta["control_group"]
    # Process json responses as csv

    def transform_comments(response):
        data = response.get("data", [])
        for record in data:
            record.pop("message_tags", None)
            record.pop("reactions", None)
        return response

    meta = process_responses(source, control_group=control_group, params_cols=["object_id", "company", "country"], transform_response=transform_comments)

    return meta


# if __name__ == '__main__':
#     load_dotenv(".secrets/meta_tokens")
#     token = os.environ["BV_PAGE_TOKEN"]
#
#     params = {"company": "BV", "country": "MX"}
#     object_ids = []
#
#     df = pd.read_csv("/home/juan.patron/PycharmProjects/social_media_comments/csv_files/campaign_ads/all_campaigns_ads_comments_BV_MX/campaign_ads_all_campaigns_ads_comments_BV_MX.csv")
#     object_ids = df["creative.effective_object_story_id"].dropna().astype(str).unique()
#
#     md = download_comments(token,object_ids,params,control_group="all_comments_BV")
#     process_comments(md)
#
#


