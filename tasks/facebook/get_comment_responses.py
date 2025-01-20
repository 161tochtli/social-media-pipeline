from sources import comment_responses
from tqdm import tqdm
from core.get_data import download_data, process_responses


def download_comments_responses(token, object_ids, params, control_group, kind='new'):
    # run download_data task for each effective_object_story_id as param
    meta = None
    for i in tqdm(object_ids):
        params.update({"parent_comment_id": i})
        meta = download_data(token, comment_responses, kind, params=params, control_group=control_group)
    return meta


def process_comment_responses(meta):
    control_group = meta["control_group"]

    def transform_comments(response):
        data = response.get("comments", {})
        return data

    # Process json responses as csv
    process_responses(comment_responses, control_group=control_group, params_cols=["parent_comment_id"], transform_response=transform_comments)

    return meta


# if __name__ == '__main__':
#     load_dotenv(".secrets/meta_tokens")
#     token = os.environ["BV_PAGE_TOKEN"]
#
#     params = {"company": "BV", "country": "MX"}
#     object_ids = []
#
#     df = pd.read_csv("/home/juan.patron/PycharmProjects/social_media_comments/csv_files/comments/all_comments_BV/comments_all_comments_BV.csv")
#     object_ids = df["object_id"].dropna().astype(str).unique()
#
#     md = download_comments_responses(token,object_ids,params,control_group="all_comments_BV")
#     process_comment_responses(md)


