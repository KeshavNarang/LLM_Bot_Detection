import os
import time
import praw
import pandas as pd
from datetime import datetime
import configparser

# === CONFIGURATION ===
NUM_COMMENTS = 1000
NUM_REPLIES = 10
BATCH_SIZE = 500  # Save every 500 comments
OUTPUT_DIR = '../data/pre_gpt'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === REDDIT AUTHENTICATION ===
config = configparser.ConfigParser()
config.read("config.ini")

reddit = praw.Reddit(
    client_id=config["REDDIT"]["client_id"],
    client_secret=config["REDDIT"]["client_secret"],
    username=config["REDDIT"]["username"],
    password=config["REDDIT"]["password"],
    user_agent=config["REDDIT"]["user_agent"]
)

# === STEP 1: LOAD ARCHIVE ===
archive_file = '../data/archive/ask_reddit.csv'

# Load the dataset
print("Loading archive dataset...")
df = pd.read_csv(archive_file)

# Ensure date format is consistent
df['created'] = pd.to_datetime(df['created'])

# Given all the posts are pre-ChatGPT (before November 30, 2022)
pre_gpt_df = df[(df['created'] < '2022-11-30') & df['title'].notna() & (df['title'] != 'Comment')]

# Select 100 unique posts
pre_gpt_posts = pre_gpt_df[['title', 'body', 'url', 'created']].drop_duplicates().head(100)
print(f"Selected {len(pre_gpt_posts)} pre-ChatGPT posts.")

# Save filtered posts to CSV for reference
pre_gpt_posts.to_csv(os.path.join(OUTPUT_DIR, "pre_gpt_posts.csv"), index=False)
print(f"Saved pre-ChatGPT post list to pre_gpt_posts.csv")

# === STEP 2: FETCH COMMENTS AND REPLIES FROM URLs ===
def save_batch(data, batch_num):
    """Save batch to CSV."""
    df = pd.DataFrame(data)
    batch_file = os.path.join(OUTPUT_DIR, f"pre_gpt_batch{batch_num}.csv")
    df.to_csv(batch_file, index=False)
    print(f"Saved batch {batch_num} with {len(data)} comments to {batch_file}")

def scrape_reddit_from_urls(post_urls):
    """Scrape comments and replies from post URLs."""
    
    data = []
    batch_num = 1

    for i, row in post_urls.iterrows():
        url = row['url']
        title = row['title']
        post_date = row['created']

        print(f"Processing post {i + 1}/{len(post_urls)}: {title}")

        try:
            submission = reddit.submission(url=url)

            # Post info
            post_id = submission.id
            post_author = str(submission.author) if submission.author else "N/A"
            post_score = submission.score
            post_created = datetime.utcfromtimestamp(submission.created_utc)

            # Expand comments
            submission.comments.replace_more(limit=10)
            top_comments = [c for c in submission.comments.list() if not c.is_root][:NUM_COMMENTS]

            # Collect comments and replies
            comment_count = 0

            for comment in top_comments:
                if comment_count >= NUM_COMMENTS:
                    break

                # Store top-level comment
                if isinstance(comment, praw.models.Comment):
                    data.append({
                        "post_id": post_id,
                        "post_title": title,
                        "post_author": post_author,
                        "post_url": url,
                        "post_score": post_score,
                        "post_created": post_created,
                        "comment_id": comment.id,
                        "comment_author": str(comment.author) if comment.author else "N/A",
                        "comment_body": comment.body,
                        "comment_score": comment.score,
                        "comment_created": datetime.utcfromtimestamp(comment.created_utc),
                        "is_reply": False,
                        "parent_id": None
                    })
                    comment_count += 1

                    # Collect replies
                    replies_collected = 0
                    for reply in comment.replies.list()[:NUM_REPLIES]:
                        if replies_collected >= NUM_REPLIES:
                            break

                        if isinstance(reply, praw.models.Comment):
                            data.append({
                                "post_id": post_id,
                                "post_title": title,
                                "post_author": post_author,
                                "post_url": url,
                                "post_score": post_score,
                                "post_created": post_created,
                                "comment_id": reply.id,
                                "comment_author": str(reply.author) if reply.author else "N/A",
                                "comment_body": reply.body,
                                "comment_score": reply.score,
                                "comment_created": datetime.utcfromtimestamp(reply.created_utc),
                                "is_reply": True,
                                "parent_id": comment.id
                            })
                            replies_collected += 1

                # Save in batches
                if len(data) >= BATCH_SIZE:
                    save_batch(data, batch_num)
                    batch_num += 1
                    data = []

            # Save remaining data
            if data:
                save_batch(data, batch_num)

        except Exception as e:
            print(f"Error processing {url}: {e}")

        # Avoid hitting Reddit's rate limit
        time.sleep(2)

# Scrape comments from pre-GPT posts
scrape_reddit_from_urls(pre_gpt_posts)

print("Pre-ChatGPT data extraction complete.")
