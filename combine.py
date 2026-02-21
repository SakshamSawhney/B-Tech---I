import pandas as pd

# Load files
posts = pd.read_csv("reddit_posts.csv")
comments = pd.read_csv("reddit_comments.csv")

# Sort comments by score (highest first)
comments = comments.sort_values(by=["post_id", "score"], ascending=[True, False])

# Group comments by post_id
grouped_comments = comments.groupby("post_id")

# Output file
output_file = "reddit_threads_readable.txt"

with open(output_file, "w", encoding="utf-8") as f:
    
    for _, post in posts.iterrows():
        post_id = post["post_id"]
        
        f.write("=" * 90 + "\n")
        f.write(f"SUBREDDIT : {post['subreddit']}\n")
        f.write(f"TITLE     : {post['title']}\n")
        f.write(f"AUTHOR    : {post['author']}\n")
        f.write(f"DATE      : {post['created_utc']}\n")
        f.write(f"SCORE     : {post['score']} | COMMENTS: {post['num_comments']}\n")
        f.write("-" * 90 + "\n\n")
        
        f.write("POST:\n")
        f.write(post["selftext"] if pd.notna(post["selftext"]) else "[No self text]")
        f.write("\n\n")
        f.write("-" * 40 + " COMMENTS " + "-" * 40 + "\n\n")
        
        if post_id in grouped_comments.groups:
            for _, comment in grouped_comments.get_group(post_id).iterrows():
                f.write(f"[+{comment['score']}] {comment['author']}:\n")
                f.write(comment["body"])
                f.write("\n\n")
        else:
            f.write("No comments scraped.\n\n")
        
        f.write("\n\n")
        
print("Readable threads saved to reddit_threads_readable.txt")