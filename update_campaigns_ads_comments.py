import pandas as pd
import datetime as dt
import argparse
import os
from dotenv import load_dotenv, dotenv_values
from core.utils import drop_duplicates, load_config
from core.get_data import get_group_control
from tasks.facebook.get_campaigns import download_campaigns, process_campaigns
from tasks.facebook.get_ads import download_campaign_ads, process_campaign_ads
from tasks.facebook.get_comments import download_comments, process_comments
from tasks.facebook.get_comment_responses import download_comments_responses, process_comment_responses

# Cargamos los valores de configuración
config_data = load_config("config.json")
wd = config_data["work_directory"]

def update_campaigns_ads_comments(brand, country, token_type, start_time=None, end_time=None, from_step=1):
    try:
        token = config_data[brand][country]["meta"][f"{token_type}_token"]
        account_id = config_data[brand][country]["meta"]["account_id"]
    except ValueError as e:
        print(f"Failed to retrieve value: Missing key '{e.args[0]}' for {brand} in {country}")
        raise

    account_name = country + "_" + brand
    print(f"Starting process for account: {account_id}")
    timestamp = f"{start_time.split('T')[0]}_{end_time.split('T')[0]}"
    control_group = f"campaigns_ads_comments_{account_name}_{timestamp}"
    params = {"account_id": account_id, "country": country, "company": brand}

    if from_step <= 1:
        print("Step 1: Download campaigns")
        md = download_campaigns(token, params, control_group, kind='new')

    if from_step <= 2:
        print("Step 2: Process campaigns")
        md = process_campaigns(md)
        drop_duplicates(md["csv_path"])

    if from_step <= 3:
        print("Step 3: Filter campaigns")
        md = get_group_control("campaigns", control_group)
        df = pd.read_csv(md["csv_path"])
        filter_status = df["status"].apply(lambda x: True)

        if start_time:
            lower_bound = dt.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S%z")
            filter_created_time_lb = df["created_time"].apply(
                lambda x: dt.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S%z")) >= lower_bound
        else:
            filter_created_time_lb = df["created_time"].apply(lambda x: True)

        if end_time:
            upper_bound = dt.datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S%z")
            filter_created_time_ub = df["created_time"].apply(
                lambda x: dt.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S%z")) <= upper_bound
        else:
            filter_created_time_ub = df["created_time"].apply(lambda x: True)

        campaigns = df[filter_status & filter_created_time_lb & filter_created_time_ub]["id"].astype(str).unique()
        print(f"{len(campaigns)} campaigns to process")

    if from_step <= 4:
        print("Step 4: Download campaign ads and creatives")
        md = download_campaign_ads(token, campaigns, params, control_group=control_group, kind='new')

    if from_step <= 5:
        print("Step 5: Process campaign ads and creatives")
        md = process_campaign_ads(md)
        drop_duplicates(md["csv_path"])

    if from_step <= 6:
        print("Step 6: Obtain ads creatives")
        md = get_group_control("campaign_ads", control_group)
        df = pd.read_csv(md["csv_path"])
        creatives = df["creative.effective_object_story_id"].dropna().astype(str).unique()
        print(f"{len(creatives)} creatives to process")

    if from_step <= 7:
        print("Step 7: Download comments on ads creatives")
        params.update({'object_id_type': 'effective_object_story_id'})
        md = download_comments(token, creatives, params, control_group, kind='new')

    if from_step <= 8:
        print("Step 8: Process comments on ads creatives")
        md = process_comments(md)
        drop_duplicates(md["csv_path"])

    if from_step <= 9:
        print("Step 9: Obtain comments requiring responses")
        md = get_group_control("comments", control_group)
        df = pd.read_csv(md["csv_path"])
        comments = df[df["comment_count"] > 0]["id"].astype(str).unique()

    if from_step <= 10:
        print("Step 10: Download comment responses")
        md = download_comments_responses(token, comments, params, control_group, kind='new')

    if from_step <= 11:
        print("Step 11: Process comment responses")
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

    update_campaigns_ads_comments(
        brand=args.brand,
        country=args.country,
        token_type=args.token_type,
        start_time=args.start_date,
        end_time=args.end_date,
        from_step=args.from_step
    )


# python3 update_ig_media_comments_NOPREFECT.py --business_unit BNF_MX --token_type page
# python3 update_campaigns_ads_comments_NOPREFECT.py --business_unit BNF_MX --token_type page
# python3 update_ig_media_comments_NOPREFECT.py --business_unit BNF_MX --token_type page

