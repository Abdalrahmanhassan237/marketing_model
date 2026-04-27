import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# ==========================================
# 1. Configuration & Master Data (The Core Links)
# ==========================================
N_ROWS = 10000
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2026, 3, 31)


def random_dates(start, end, n):
    delta = end - start
    return [start + timedelta(days=random.randint(0, delta.days)) for _ in range(n)]


# Master Keys for Relational Integrity
campaign_names = [f"Campaign_00{i}" for i in range(1, 51)]  # 50 Unique Campaigns
utm_sources = [
    "fb_reels_promo",
    "fb_story_sale",
    "ig_feed_awareness",
    "organic_bio_link",
    "influencer_shoutout",
]
customer_ids = [
    f"CUST-{str(i).zfill(5)}" for i in range(1, 8001)
]  # 8000 Unique Customers

# ==========================================
# 2. Generating Clean Data DataFrames
# ==========================================
print("Generating clean data...")

# --- Table 1: fb_ads ---
fb_ads = pd.DataFrame(
    {
        "Date": random_dates(START_DATE, END_DATE, N_ROWS),
        "Campaign_Name": np.random.choice(campaign_names, N_ROWS),
        "Placement": np.random.choice(["Reels", "Story", "Feed"], N_ROWS),
        "Target_Audience": np.random.choice(
            ["Lookalike", "Broad", "Retargeting"], N_ROWS
        ),
        "Impressions": np.random.randint(1000, 50000, N_ROWS),
        "Clicks": np.random.randint(50, 5000, N_ROWS),
        "Spend": np.round(np.random.uniform(10.0, 500.0), 2),
        "Conversions": np.random.randint(0, 100, N_ROWS),
        "UTM_Campaign": np.random.choice(utm_sources[:3], N_ROWS),  # Paid sources
    }
)

# --- Table 2: fb_content ---
fb_content = pd.DataFrame(
    {
        "Date": random_dates(START_DATE, END_DATE, N_ROWS),
        "Post_Type": np.random.choice(["Reel", "Carousel", "Image", "Video"], N_ROWS),
        "Reach": np.random.randint(500, 100000, N_ROWS),
        "Watch_Time_Mins": np.round(np.random.uniform(0.5, 100.0), 1),
        "Likes": np.random.randint(10, 5000, N_ROWS),
        "Comments": np.random.randint(0, 500, N_ROWS),
        "Shares": np.random.randint(0, 1000, N_ROWS),
        "Saves": np.random.randint(0, 1500, N_ROWS),
        "New_Followers": np.random.randint(0, 300, N_ROWS),
        "UTM_Campaign": np.random.choice(utm_sources[3:], N_ROWS),  # Organic sources
    }
)

# --- Table 3: fb_sales ---
fb_sales = pd.DataFrame(
    {
        "Date": random_dates(START_DATE, END_DATE, N_ROWS),
        "Order_ID": [f"ORD-{str(i).zfill(6)}" for i in range(1, N_ROWS + 1)],
        "Customer_ID": np.random.choice(customer_ids, N_ROWS),
        "Revenue": np.round(np.random.uniform(20.0, 1500.0), 2),
        "Product_Category": np.random.choice(
            ["Course", "Consulting", "Template", "E-book"], N_ROWS
        ),
        "Customer_Type": np.random.choice(["New", "Returning"], N_ROWS, p=[0.7, 0.3]),
        "Promo_Code": np.random.choice(["SAVE10", "WELCOME", "NULL"], N_ROWS),
        "Traffic_Source": np.random.choice(utm_sources + ["direct_traffic"], N_ROWS),
    }
)

# --- Table 4: fb_customers ---
fb_customers = pd.DataFrame(
    {
        "Customer_ID": np.random.choice(customer_ids, N_ROWS),
        "Age": np.random.randint(18, 65, N_ROWS),
        "Gender": np.random.choice(["Male", "Female", "Prefer not to say"], N_ROWS),
        "Location": np.random.choice(
            ["Cairo", "Alexandria", "Dubai", "Riyadh", "Jeddah"], N_ROWS
        ),
        "Device_Type": np.random.choice(["iOS", "Android", "Desktop"], N_ROWS),
        "Account_Creation_Date": random_dates(datetime(2022, 1, 1), END_DATE, N_ROWS),
    }
)

# --- Table 5: fb_ad_creatives ---
fb_ad_creatives = pd.DataFrame(
    {
        "Campaign_Name": np.random.choice(campaign_names, N_ROWS),
        "Creative_Format": np.random.choice(
            ["UGC", "Animation", "Talking_Head", "Text_Based"], N_ROWS
        ),
        "Video_Duration_Sec": np.random.choice([15, 30, 60, np.nan], N_ROWS),
        "Hook_Type": np.random.choice(
            ["Emotional", "Logical", "Question", "Discount"], N_ROWS
        ),
        "CTA_Type": np.random.choice(
            ["Buy_Now", "Learn_More", "Sign_Up", "Download"], N_ROWS
        ),
    }
)

# ==========================================
# 3. The "Messifier" (Injecting 45% Chaos)
# ==========================================
print("Injecting chaos (messy data)...")


def make_messy(df):
    df_messy = df.copy()
    num_rows = len(df_messy)

    # We will iterate and apply issues to roughly 45% of rows randomly
    for col in df_messy.columns:
        # 1. Null Injection (5% per column)
        null_indices = df_messy.sample(frac=0.05).index
        df_messy.loc[null_indices, col] = np.nan

        # 2. String formatting inconsistencies (for object/string columns)
        if df_messy[col].dtype == "object" and col not in ["Order_ID"]:
            format_indices = df_messy.sample(frac=0.10).index
            # Add random spaces or change case to test SQL TRIM() and UPPER()
            df_messy.loc[format_indices, col] = df_messy.loc[format_indices, col].apply(
                lambda x: f"  {str(x).upper()} " if pd.notnull(x) else x
            )

        # 3. Type mismatches in numeric columns (injecting text like "N/A" or "Error")
        if pd.api.types.is_numeric_dtype(df_messy[col]) and col not in [
            "Date",
            "Account_Creation_Date",
        ]:
            mismatch_indices = df_messy.sample(frac=0.02).index
            df_messy[col] = df_messy[col].astype(object)  # Force to object to hold text
            df_messy.loc[mismatch_indices, col] = np.random.choice(
                ["N/A", "Error", "Missing"]
            )

        # 4. Logical Outliers (Negative values in Revenue, Spend, Clicks)
        if col in ["Revenue", "Spend", "Clicks", "Impressions"]:
            outlier_indices = df_messy.sample(frac=0.03).index
            # Force negative values to test absolute/filtering logic
            df_messy.loc[outlier_indices, col] = -999.99

    return df_messy


# Apply messiness
fb_ads_messy = make_messy(fb_ads)
fb_content_messy = make_messy(fb_content)
fb_sales_messy = make_messy(fb_sales)
fb_customers_messy = make_messy(fb_customers)
fb_ad_creatives_messy = make_messy(fb_ad_creatives)

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# ==========================================
# 1. Configuration & Master Data
# ==========================================
N_ROWS = 10000
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2026, 3, 31)


def random_dates(start, end, n):
    delta = end - start
    return [start + timedelta(days=random.randint(0, delta.days)) for _ in range(n)]


campaign_names = [f"Campaign_00{i}" for i in range(1, 51)]
utm_sources = [
    "fb_reels_promo",
    "fb_story_sale",
    "ig_feed_awareness",
    "organic_bio_link",
    "influencer_shoutout",
]
customer_ids = [f"CUST-{str(i).zfill(5)}" for i in range(1, 8001)]

# ==========================================
# 2. Generating Data (Core Logic)
# ==========================================
print("Generating raw data...")

# Table 1: fb_ads
fb_ads = pd.DataFrame(
    {
        "Date": random_dates(START_DATE, END_DATE, N_ROWS),
        "Campaign_Name": np.random.choice(campaign_names, N_ROWS),
        "Placement": np.random.choice(["Reels", "Story", "Feed"], N_ROWS),
        "Target_Audience": np.random.choice(
            ["Lookalike", "Broad", "Retargeting"], N_ROWS
        ),
        "Impressions": np.random.randint(1000, 50000, N_ROWS),
        "Clicks": np.random.randint(50, 5000, N_ROWS),
        "Spend": np.round(np.random.uniform(10.0, 500.0, N_ROWS), 2),
        "Conversions": np.random.randint(0, 100, N_ROWS),
        "UTM_Campaign": np.random.choice(utm_sources[:3], N_ROWS),
    }
)

# Table 2: fb_content
fb_content = pd.DataFrame(
    {
        "Date": random_dates(START_DATE, END_DATE, N_ROWS),
        "Post_Type": np.random.choice(["Reel", "Carousel", "Image", "Video"], N_ROWS),
        "Reach": np.random.randint(500, 100000, N_ROWS),
        "Watch_Time_Mins": np.round(np.random.uniform(0.5, 100.0, N_ROWS), 1),
        "Likes": np.random.randint(10, 5000, N_ROWS),
        "Comments": np.random.randint(0, 500, N_ROWS),
        "Shares": np.random.randint(0, 1000, N_ROWS),
        "Saves": np.random.randint(0, 1500, N_ROWS),
        "New_Followers": np.random.randint(0, 300, N_ROWS),
        "UTM_Campaign": np.random.choice(utm_sources[3:], N_ROWS),
    }
)

# Table 3: fb_sales
fb_sales = pd.DataFrame(
    {
        "Date": random_dates(START_DATE, END_DATE, N_ROWS),
        "Order_ID": [f"ORD-{str(i).zfill(6)}" for i in range(1, N_ROWS + 1)],
        "Customer_ID": np.random.choice(customer_ids, N_ROWS),
        "Revenue": np.round(np.random.uniform(20.0, 1500.0, N_ROWS), 2),
        "Product_Category": np.random.choice(
            ["Course", "Consulting", "Template", "E-book"], N_ROWS
        ),
        "Customer_Type": np.random.choice(["New", "Returning"], N_ROWS, p=[0.7, 0.3]),
        "Promo_Code": np.random.choice(["SAVE10", "WELCOME", "NULL"], N_ROWS),
        "Traffic_Source": np.random.choice(utm_sources + ["direct_traffic"], N_ROWS),
    }
)

# Table 4: fb_customers
fb_customers = pd.DataFrame(
    {
        "Customer_ID": np.random.choice(customer_ids, N_ROWS),
        "Age": np.random.randint(18, 65, N_ROWS),
        "Gender": np.random.choice(["Male", "Female", "Prefer not to say"], N_ROWS),
        "Location": np.random.choice(
            ["Cairo", "Alexandria", "Dubai", "Riyadh", "Jeddah"], N_ROWS
        ),
        "Device_Type": np.random.choice(["iOS", "Android", "Desktop"], N_ROWS),
        "Account_Creation_Date": random_dates(datetime(2022, 1, 1), END_DATE, N_ROWS),
    }
)

# Table 5: fb_ad_creatives
fb_ad_creatives = pd.DataFrame(
    {
        "Campaign_Name": np.random.choice(campaign_names, N_ROWS),
        "Creative_Format": np.random.choice(
            ["UGC", "Animation", "Talking_Head", "Text_Based"], N_ROWS
        ),
        "Video_Duration_Sec": np.random.choice([15, 30, 60, np.nan], N_ROWS),
        "Hook_Type": np.random.choice(
            ["Emotional", "Logical", "Question", "Discount"], N_ROWS
        ),
        "CTA_Type": np.random.choice(
            ["Buy_Now", "Learn_More", "Sign_Up", "Download"], N_ROWS
        ),
    }
)


# ==========================================
# 3. The Messifier Function
# ==========================================
def make_messy(df):
    df_messy = df.copy()
    for col in df_messy.columns:
        # Null Injection
        null_indices = df_messy.sample(frac=0.05).index
        df_messy.loc[null_indices, col] = np.nan

        # Formatting inconsistencies
        if df_messy[col].dtype == "object" and col not in ["Order_ID", "Customer_ID"]:
            format_indices = df_messy.sample(frac=0.10).index
            df_messy.loc[format_indices, col] = df_messy.loc[format_indices, col].apply(
                lambda x: f"  {str(x).upper()} " if pd.notnull(x) else x
            )

        # Type mismatches (Injecting "Error/NA" strings into numbers)
        if pd.api.types.is_numeric_dtype(df_messy[col]) and col not in [
            "Date",
            "Account_Creation_Date",
        ]:
            mismatch_indices = df_messy.sample(frac=0.03).index
            df_messy[col] = df_messy[col].astype(object)
            df_messy.loc[mismatch_indices, col] = "N/A"

        # Logical Outliers
        if col in ["Revenue", "Spend", "Clicks"]:
            outlier_indices = df_messy.sample(frac=0.03).index
            df_messy.loc[outlier_indices, col] = -1.0

    return df_messy


# Apply messy logic
print("Applying messy logic...")
fb_ads_final = make_messy(fb_ads)
fb_content_final = make_messy(fb_content)
fb_sales_final = make_messy(fb_sales)
fb_customers_final = make_messy(fb_customers)
fb_ad_creatives_final = make_messy(fb_ad_creatives)

# ==========================================
# 4. Exporting Each DataFrame to its own File
# ==========================================
print("Exporting separate files...")

fb_ads_final.to_excel("fb_ads.xlsx", index=False)
fb_content_final.to_excel("fb_content.xlsx", index=False)
fb_sales_final.to_excel("fb_sales.xlsx", index=False)
fb_customers_final.to_excel("fb_customers.xlsx", index=False)
fb_ad_creatives_final.to_excel("fb_ad_creatives.xlsx", index=False)

print(
    "Success! 5 separate files created: fb_ads.xlsx, fb_content.xlsx, fb_sales.xlsx, fb_customers.xlsx, fb_ad_creatives.xlsx"
)
