# main_scraper.py

import praw
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import time
import logging
from tqdm import tqdm
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta   # <-- NEW

# --- 1. CONFIGURATION ---
CONFIG = {
    "subreddits": [
        "IndiaSpeaks",
        "India",
        "IndiaNews",
        "IndianLeft",
        "DesiPolitics",
        "Bharat",
        "PoliticalAnalysisIndia",
        "ElectionFever"
    ],
    "search_keywords": [
    "Lok Sabha election 2024",
    "Lok Sabha 2024",
    "general elections 2024",
    "voting 2024",
    "exit polls 2024",
    "election campaign",
    "election results",
    
    "BJP",
    "Congress",
    "INDIA alliance",
    "NDA",
    "AAP",
    "SP",
    "DMK",
    
    "Narendra Modi",
    "Rahul Gandhi",
    "Arvind Kejriwal",
    "Akhilesh Yadav",
    "MK Stalin",
    "Sharad Pawar",
    "Tejashwi Yadav",
    "Mamata Banerjee",
    "Yogi Adityanath",
    
    "Shiv Sena",
    "Shiv Sena split",
    "AIADMK",
    "BSP",
    "INC",
    "CPI",
    
    "farmers protest",
    "CAA NRC",
    "inflation India",
    "corruption India",
    "unemployment India",
    "MSP farmers",
    
    "Ram Mandir",
    "Bharat Jodo Yatra",
    
    "election manifesto",
    "development politics India",
    "Kejriwal arrest",
    
    "booth capturing",
    "roadshow election",
    "EVM",
    "VVPAT"
]
,
    "limit_posts_per_query": 1000,
    "comment_limit_per_post": 10,
    "output_posts_file": "reddit_posts.csv",
    "output_comments_file": "reddit_comments.csv"
}

# --- 2. LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log", mode='w'),
        logging.StreamHandler()
    ]
)

class RedditScraper:
    def __init__(self):
        """Initializes the scraper, loads API credentials, and connects to Reddit."""
        load_dotenv()
        client_id = os.getenv("REDDIT_CLIENT_ID")
        client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        user_agent = os.getenv("REDDIT_USER_AGENT")

        if not all([client_id, client_secret, user_agent]):
            logging.error("Missing Reddit API credentials in .env file.")
            raise ValueError("Reddit API credentials not found.")

        try:
            self.reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent
            )
            logging.info(f"Connected to Reddit API | Read-only: {self.reddit.read_only}")
        except Exception as e:
            logging.error(f"Failed to create Reddit instance: {e}")
            raise

        self.processed_post_ids = self._load_processed_ids()

    def _load_processed_ids(self):
        """Loads IDs of already scraped posts to prevent duplicate work."""
        if not os.path.exists(CONFIG['output_posts_file']):
            return set()
        try:
            df = pd.read_csv(CONFIG['output_posts_file'])
            return set(df['post_id'].unique())
        except pd.errors.EmptyDataError:
            return set()

    def _fetch_comments(self, post):
        """Fetch and process comments for a post."""
        comments_data = []
        try:
            post.comments.replace_more(limit=0)
            comment_iterator = post.comments.list()

            for i, comment in enumerate(comment_iterator):
                if i >= CONFIG['comment_limit_per_post']:
                    break

                comments_data.append({
                    "comment_id": comment.id,
                    "post_id": post.id,
                    "body": comment.body,
                    "score": comment.score,
                    "author": str(comment.author),
                    "created_utc": datetime.fromtimestamp(post.created_utc, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                })
        except Exception as e:
            logging.warning(f"Could not fetch comments for post {post.id}: {e}")
        return comments_data

    def scrape_data(self):
        """Main scraping logic."""
        logging.info("Starting data collection...")

        # --- NEW: Election-based timeframe ---
        election_date = datetime(2024, 6, 4)  # Lok Sabha result day
        start_date = election_date - relativedelta(months=5)  # 5 months before
        end_date = election_date + relativedelta(months=2)    # 2 month after

        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())

        logging.info(f"Filtering posts from {start_date.date()} to {end_date.date()}")

        total_searches = len(CONFIG['subreddits']) * len(CONFIG['search_keywords'])
        pbar = tqdm(total=total_searches, desc="Overall Progress")

        for subreddit_name in CONFIG['subreddits']:
            for query in CONFIG['search_keywords']:
                pbar.set_description(f"r/{subreddit_name} - '{query}'")

                posts_batch = []
                comments_batch = []

                try:
                    full_query = f"{query} timestamp:{start_timestamp}..{end_timestamp}"
                    subreddit = self.reddit.subreddit(subreddit_name)

                    search_results = subreddit.search(
                        full_query,
                        limit=CONFIG['limit_posts_per_query'],
                        syntax='cloudsearch'
                    )

                    for post in search_results:
                        if post.id in self.processed_post_ids:
                            continue

                        # --- NEW: Manual timestamp filtering ---
                        post_timestamp = int(post.created_utc)
                        if post_timestamp < start_timestamp or post_timestamp > end_timestamp:
                            continue  # Skip posts outside the date window

                        posts_batch.append({
                            "post_id": post.id,
                            "subreddit": subreddit_name,
                            "keyword": query,
                            "title": post.title,
                            "selftext": post.selftext,
                            "score": post.score,
                            "upvote_ratio": post.upvote_ratio,
                            "num_comments": post.num_comments,
                            "url": post.url,
                            "author": str(post.author),
                            "created_utc": datetime.fromtimestamp(post.created_utc, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                        })

                        # Fetch comments
                        comments = self._fetch_comments(post)
                        comments_batch.extend(comments)

                        self.processed_post_ids.add(post.id)

                    # Save in batches
                    if posts_batch:
                        self._save_data(posts_batch, CONFIG['output_posts_file'])
                    if comments_batch:
                        self._save_data(comments_batch, CONFIG['output_comments_file'])

                    time.sleep(2)

                except Exception as e:
                    logging.error(f"Error for '{query}' in r/{subreddit_name}: {e}")

                pbar.update(1)

        pbar.close()
        logging.info("Data collection complete.")

    def _save_data(self, data, filename):
        """Save batch to CSV."""
        if not data:
            return

        is_new = not os.path.exists(filename)
        df = pd.DataFrame(data)

        df.to_csv(
            filename,
            mode='a',
            header=is_new,
            index=False,
            encoding="utf-8-sig"
        )

        logging.info(f"Saved {len(df)} rows to {filename}")


# --- 3. MAIN EXECUTION ---
if __name__ == "__main__":
    scraper = RedditScraper()
    scraper.scrape_data()
    logging.info("--- Scraping script finished ---")
