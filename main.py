import os
import json
import time
import random
import requests
from datetime import datetime

# ==============================
# Config
# ==============================
RAW_FILE = "data/posts.jsonl"
RESULT_FILE = "data/results.jsonl"

# ...existing code...
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PERSPECTIVE_API_KEY = os.getenv("PERSPECTIVE_API_KEY")


if not OPENAI_API_KEY:
    raise ValueError("⚠️ Please set OPENAI_API_KEY environment variable.")
if not PERSPECTIVE_API_KEY:
    raise ValueError("⚠️ Please set PERSPECTIVE_API_KEY environment variable.")

# ==============================
# 4CHAN DATA COLLECTION
# ==============================
def fetch_catalog():
    url = "https://a.4cdn.org/pol/catalog.json"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

def fetch_thread(thread_id):
    url = f"https://a.4cdn.org/pol/thread/{thread_id}.json"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

def clean_comment(comment):
    if not comment:
        return ""
    text = comment.replace("<br>", "\n")
    text = text.replace("&gt;", ">").replace("&quot;", '"')
    return text

def save_posts(posts, filename):
    with open(filename, "a", encoding="utf-8") as f:
        for p in posts:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")

# ==============================
# 1. Collect posts (dummy simulation)
# ==============================
def collect_posts(limit=5000):
    seen_posts = set()
    collected = 0

    while collected < limit:
        try:
            catalog = fetch_catalog()
            for page in catalog:
                for thread in page["threads"]:
                    tid = thread["no"]
                    try:
                        thread_data = fetch_thread(tid)
                        for post in thread_data["posts"]:
                            pid = post["no"]
                            if pid in seen_posts:
                                continue
                            seen_posts.add(pid)
                            collected += 1

                            record = {
                                "thread_id": tid,
                                "post_id": pid,
                                "timestamp": post["time"],
                                "comment": clean_comment(post.get("com", "")),
                                "metadata": {
                                    "board": "pol",
                                    "scraped_at": datetime.utcnow().isoformat()
                                }
                            }
                            save_posts([record], RAW_FILE)

                            if collected >= limit:
                                break
                        time.sleep(1)  # rate limit
                    except Exception as e:
                        print(f"Thread fetch failed: {e}")
                        time.sleep(2)
                if collected >= limit:
                    break
        except Exception as e:
            print(f"Catalog fetch failed: {e}")
            time.sleep(5)

    print(f"✅ Collected {collected} posts into {RAW_FILE}")

# ==============================
# 2. OpenAI Moderation API
# ==============================
def analyze_openai(text, max_retries=5):
    url = "https://api.openai.com/v1/moderations"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": "omni-moderation-latest", "input": text}

    for attempt in range(max_retries):
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 200:
            return resp.json()["results"][0]
        elif resp.status_code == 429:  # rate limit
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            print(f"⚠️ Rate limit hit. Retrying in {wait_time:.2f} sec...")
            time.sleep(wait_time)
        else:
            print("❌ OpenAI error:", resp.status_code, resp.text)
            return None
    return None

# ==============================
# 3. Google Perspective API
# ==============================
def analyze_perspective(text):
    url = f"https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze?key={PERSPECTIVE_API_KEY}"
    data = {
        "comment": {"text": text},
        "languages": ["en"],
        "requestedAttributes": {"TOXICITY": {}}
    }
    try:
        resp = requests.post(url, json=data)
        if resp.status_code == 200:
            return resp.json()
        else:
            print("❌ Perspective error:", resp.status_code, resp.text)
            return None
    except Exception as e:
        print("❌ Perspective exception:", e)
        return None

# ==============================
# 4. Analysis Function (batched)
# ==============================
def analyze_posts_batched(input_file=RAW_FILE, output_file=RESULT_FILE, limit=1000, batch_size=5):
    with open(input_file, "r", encoding="utf-8") as f, \
         open(output_file, "a", encoding="utf-8") as out:

        posts = []
        for i, line in enumerate(f):
            if i >= limit:
                break
            post = json.loads(line)
            text = post.get("comment", "").strip()
            if not text:
                continue
            post["text"] = text
            posts.append(post)

        # Process in batches
        for start in range(0, len(posts), batch_size):
            batch_posts = posts[start:start+batch_size]
            print(f"🔍 Analyzing batch {start//batch_size + 1} ({len(batch_posts)} posts)")

            for post in batch_posts:
                oa_result = analyze_openai(post["text"])
                persp_result = analyze_perspective(post["text"])

                result = {
                    "post_id": post["post_id"],
                    "thread_id": post["thread_id"],
                    "timestamp": post["timestamp"],
                    "text": post["text"],
                    "openai": oa_result,
                    "perspective": persp_result,
                    "analyzed_at": datetime.utcnow().isoformat()
                }
                out.write(json.dumps(result, ensure_ascii=False) + "\n")

                # small cooldown per request
                time.sleep(1)

            print("⏸ Cooling down for 5 sec before next batch...")
            time.sleep(5)

    print(f"✅ Analysis complete. Results saved to {output_file}")

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    # Step 1: Collect posts (5000 by default)
    collect_posts(limit=5000)

    # Step 2: Analyze subset (100 posts by default)
    analyze_posts_batched(limit=1000, batch_size=5)
