import pandas as pd
import argparse
import os
from core.utils import drop_duplicates, load_config
from core.get_data import get_group_control
from tasks.facebook.get_feed import download_feed_posts, process_feed_posts
from tasks.facebook.get_comments import download_comments, process_comments
from tasks.facebook.get_comment_responses import download_comments_responses, process_comment_responses

# Cargamos los valores de configuración
config_data = load_config("config.json")
wd = config_data["work_directory"]

def update_feed_comments(brand, country, token_type, start_time=None, end_time=None, from_step=1):
    try:
        token = config_data[brand][country]["meta"][f"{token_type}_token"]
        page_id = config_data[brand][country]["meta"]["page_id"]
    except ValueError as e:
        print(f"Failed to retrieve value: Missing key '{e.args[0]}' for {brand} in {country}")
        raise

    account_name = country + "_" + brand
    print(f"Starting process for account: {account_name}")
    account_name = brand + "_" + country
    timestamp = f"{start_time.split('T')[0]}_{end_time.split('T')[0]}"
    control_group = f"feed_comments_{account_name}_{timestamp}"
    params = {"page_id": page_id, "country": country, "company": brand}

    if from_step <= 1:
        print("Step 1: Download feed posts")
        md = download_feed_posts(token, params, control_group, kind='new')

    if from_step <= 2:
        print("Step 2: Process feed posts")
        md = process_feed_posts(md)
        drop_duplicates(md["csv_path"])
        df = pd.read_csv(md["csv_path"])
        feed_posts = df["id"].dropna().astype(str).unique()

    if from_step <= 3:
        print("Step 3: Download comments on feed posts")
        # params.update({'object_id_type': 'effective_object_story_id'})
        md = download_comments(token, feed_posts, params, control_group, kind='new')

    if from_step <= 4:
        print("Step 4: Process comments on feed posts")
        md = process_comments(md)
        drop_duplicates(md["csv_path"])

    if from_step <= 5:
        print("Step 5: Obtain comments with responses")
        md = get_group_control("comments", control_group)
        df = pd.read_csv(md["csv_path"])
        comments = df[df["comment_count"] > 0]["id"].astype(str).unique()

    if from_step <= 6:
        print("Step 6: Download comment responses")
        md = download_comments_responses(token, comments, params, control_group, kind='new')

    if from_step <= 7:
        print("Step 7: Process comment responses")
        md = process_comment_responses(md)
        drop_duplicates(md["csv_path"])

    print("Process completed successfully.")


if __name__ == '__main__':
    start_date = "2015-01-01T00:00:00-0600"
    end_date = "2025-01-15T23:59:59-0600"

    parser = argparse.ArgumentParser(description="Update campaigns, ads, and comments data.")
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
        token_type=args.token_type,
        start_time=args.start_date,
        end_time=args.end_date,
        from_step=args.from_step
    )
