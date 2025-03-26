import time
import praw
import pandas as pd
import configparser
import os
from datetime import datetime

# === CONFIGURATION ===
NUM_POSTS = 100
NUM_COMMENTS = 1000
NUM_REPLIES = 10
BATCH_SIZE = 500  # Save every 500 comments
OUTPUT_DIR = './data/raw'
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

# === HELPER FUNCTION: SAVE BATCH ===
def save_batch(data, filename, batch_num):
    """Save batch to CSV."""
    df = pd.DataFrame(data)
    batch_file = os.path.join(OUTPUT_DIR, f"{filename}_batch{batch_num}.csv")
    df.to_csv(batch_file, index=False)
    print(f"Saved batch {batch_num} with {len(data)} comments to {batch_file}")

# === FUNCTION TO SCRAPE REDDIT ===
def scrape_reddit(subreddit_name, sort_by="top"):
    """Scrape Reddit posts + comments, save in batches."""
    
    data = []
    batch_num = 1

    subreddit = reddit.subreddit(subreddit_name)
    posts = subreddit.top(limit=NUM_POSTS) if sort_by == "top" else subreddit.hot(limit=NUM_POSTS)

    print(f"Scraping {sort_by} posts...")

    for i, submission in enumerate(posts, start=1):
        print(f"Processing post {i}/{NUM_POSTS}: {submission.title}")

        try:
            # Post details
            post_id = submission.id
            post_title = submission.title
            post_author = str(submission.author) if submission.author else 'N/A'
            post_url = submission.url
            post_score = submission.score
            post_created = datetime.utcfromtimestamp(submission.created_utc)

            # Expand comments
            submission.comments.replace_more(limit=10)
            top_comments = [c for c in submission.comments.list() if not c.is_root][:NUM_COMMENTS]

            # Collect comments + replies
            comment_count = 0
            for comment in top_comments:
                if comment_count >= NUM_COMMENTS:
                    break

                # Top-level comment
                if isinstance(comment, praw.models.Comment):
                    data.append({
                        "post_id": post_id,
                        "post_title": post_title,
                        "post_author": post_author,
                        "post_url": post_url,
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
                                "post_title": post_title,
                                "post_author": post_author,
                                "post_url": post_url,
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
                    save_batch(data, sort_by, batch_num)
                    batch_num += 1
                    data = []

            # Save remaining data
            if data:
                save_batch(data, sort_by, batch_num)

        except Exception as e:
            print(f"Error processing post {i}: {e}")

        # Avoid hitting Reddit's rate limit
        time.sleep(2)

# === EXECUTE SCRAPER ===
scrape_reddit("askreddit", "top")
scrape_reddit("askreddit", "hot")

print("Scraping complete.")
