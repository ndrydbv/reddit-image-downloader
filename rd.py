#!/usr/bin/env python3

import requests, datetime
import argparse, sys
import json
import os, sys
import time
import logging


args = {}
url_list = []
uniq_sizes = set()

# pushshift helper function
def get_posts(post_type,params, cb, limit=-1):
    #if a limit was specified by the user, set the size variable
    if limit != -1:
        if limit >= 100:
            #pushshift caps requests at 100 so if the limit is more than 100, we'll have to do multiple passes
            size = 100
        else:
            size = limit
    else:
        size = 100
    last = int(datetime.datetime.now().timestamp())
    got = 0
    while True:
        logging.info(f"Fetching posts made before {last}")
        req_params = {
                **params,
                'size':size,
                'before':last
                }
        req_headers = {
                'User-Agent':'Python requests - Redditstat.py'
                }
        res = requests.get(f'https://api.pushshift.io/reddit/{post_type}/search', params=req_params, headers=req_headers)
        res.raise_for_status()
        data = res.json()["data"]
        cb(data)
        #stop fetching posts if we've there aren't any more or if we've hit the limit
        if len(data) < 100 or (limit != -1 and got >= limit):
            got += len(data)
            logging.info(f"Total of {got} posts fetched from u/{params['author']}")
            return
        else:
            last = data[-1]["created_utc"]
            got += 100

def submission_callback(data):
    print (len(data))
    for post in data:
        process_submission(post)

def process_submission(post):
    global url_list
    global uniq_sizes
    try:
        if not post['is_self'] and post['url'] not in url_list:
            if not post['is_video'] and "gif" not in post['url']:
                try:
                    res = requests.get(post['url'])
                    if(res):
                        print("Downloading file")
                        print (post['url'])
                        target_file = os.path.join("output",post['author'], f"{datetime.datetime.now().strftime('%Y-%m-%dT%H%M%S')}-{post['url'].split('/')[-1]}")
                        with open(target_file, "wb+") as f:
                            f.write(res.content)
                            logging.info(f"Photo downloaded from {post['url']} and saved to {f.name}")

                        #cheap uniqueness check
                        if os.path.getsize(target_file) in uniq_sizes:
                            os.remove(target_file)
                            print('Remove duplicate')
                        uniq_sizes.add(os.path.getsize(target_file))

                except Exception:
                    logging.error(f"Exception downloading {post['url']}.  Skipping.")

            else:
                print("Skip video")
    except KeyError:
        print("What?")
    url_list.append(post['url'])


def main():
    global args
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level='INFO', filename='execution.log')
    #logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level='DEBUG', stream=sys.stdout)
    parser = argparse.ArgumentParser(description="Download reddit media")
    parser.add_argument('-u', '--user', help="USER to download from")
    parser.add_argument('-s', '--subreddit', help="SUBREDDIT to download from")
    parser.add_argument('-l','--limit',help="Maximum number of posts to be downloaded")
    parser.add_argument('--pushshift-params', help="JSON-formatted pushshift parameters", default='{}')
    args = parser.parse_args()
    logging.info(f"\n\n{'-'*30}\nBeginning download of media from user u/{args.user}")

    #get working directory
    cwd = os.getcwd()
    images_dir = os.path.join(cwd,"output", args.user)

    #create the folder for the user if it doesn't exist
    try:
       os.makedirs(images_dir)
       logging.info(f"Created folder for reddit user {args.user}")
    except OSError as e:
       logging.info(f"Folder already exists for reddit user {args.user}")
       print (e)
    if args.limit:
        get_posts('submission', {**json.loads(args.pushshift_params), 'subreddit':args.subreddit, 'author':args.user}, submission_callback, int(args.limit))
    else:
        get_posts('submission', {**json.loads(args.pushshift_params), 'subreddit':args.subreddit, 'author':args.user}, submission_callback)
    print("\n\n")
    logging.info("Execution complete. Exiting...")
    sys.exit()


if __name__ == "__main__":
    main()
