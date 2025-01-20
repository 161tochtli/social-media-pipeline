import pandas as pd
import argparse
from core.utils import drop_duplicates, load_config
from core.get_data import get_group_control
from tasks.instagram.get_ig_media import download_ig_media, process_ig_media
from tasks.instagram.get_ig_comments import download_ig_comments, process_ig_comments
from tasks.instagram.get_ig_comment_replies import download_ig_comment_replies, process_ig_comment_replies

# Cargamos los valores de configuración
config_data = load_config()
wd = config_data["work_directory"]

def update_feed_comments(brand, country, start_time=None, end_time=None, from_step=1):
    try:
        token = config_data[brand][country]["meta"]["page_token"]
        user_id = config_data[brand][country]["meta"]["ig_page_id"]
    except ValueError as e:
        print(f"Failed to retrieve value: Missing key '{e.args[0]}' for {brand} in {country}")
        raise

    account_name = country+"_"+brand
    print(f"Starting process for account: {account_name}")
    timestamp = f"{start_time.split('T')[0]}_{end_time.split('T')[0]}"
    control_group = f"ig_media_comments_{account_name}_{timestamp}"

    params = {"user_id": user_id, "country": country, "brand": brand}

    if from_step <= 1:
        print("Step 1: Download ig media")
        md = download_ig_media(token, params, control_group, kind='new')

    if from_step <= 2:
        print("Step 2: Process ig_media")
        md = process_ig_media(md)
        drop_duplicates(md["csv_path"])
        df = pd.read_csv(md["csv_path"])
        # ### TESTING
        df = df.head(10)
        # TESTING ###
        media_ids = df[df["comments_count"]>0]["id"].dropna().astype(str).unique()

    if from_step <= 3:
        print("Step 3: Download comments on ig media")
        # params.update({'object_id_type': 'effective_object_story_id'})
        md = download_ig_comments(token, media_ids, params, control_group, kind='new')

    if from_step <= 4:
        print("Step 4: Process comments on ig media")
        md = process_ig_comments(md)
        drop_duplicates(md["csv_path"])

    if from_step <= 5:
        print("Step 5: Obtain comments with responses")
        md = get_group_control("ig_comments", control_group)
        df = pd.read_csv(md["csv_path"])
        comments = df["id"]

    if from_step <= 6:
        print("Step 6: Download comment replies")
        md = download_ig_comment_replies(token, comments, params, control_group, kind='new')

    if from_step <= 7:
        print("Step 7: Process comment replies")
        md = process_ig_comment_replies(md)
        drop_duplicates(md["csv_path"])

    print("Process completed successfully.")


if __name__ == '__main__':
    start_date = "2015-01-01T00:00:00-0600"
    end_date = "2025-01-16T23:59:59-0600"

    parser = argparse.ArgumentParser(description="Update instagram media and comments.")
    parser.add_argument("--country", required=True, help="Business unit (e.g., MX, CL, CO)")
    parser.add_argument("--brand", required=True, help="Brand (e.g., BNF, BV)")
    parser.add_argument("--token_type", required=True, choices=["page", "user"], help="Token type (page or user)")
    parser.add_argument("--start_date", default=start_date, help="Start date in ISO format")
    parser.add_argument("--end_date", default=end_date, help="End date in ISO format")
    parser.add_argument("--from_step", type=int, default=1, help="Start from a specific step (default: 1)")

    args = parser.parse_args()

    update_feed_comments(
        brand=args.brand,
        country=args.country,
        start_time=args.start_date,
        end_time=args.end_date,
        from_step=args.from_step
    )
