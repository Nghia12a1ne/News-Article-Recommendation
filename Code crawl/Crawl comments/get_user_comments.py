import requests
import os
import pandas as pd
import re
import json
import time
import random
import threading

USER_URL = "https://usi-saas.vnexpress.net/api/comment/bytime?user_id={}&offset={}&limit={}"


def make_request(url, count=0):
    time.sleep(1)
    try:
        r = requests.get(url)
        if r.status_code == 200:
            return r
        else:
            count += 1
            if count < 3:
                time.sleep(random.randint(3, 6))
                return make_request(url, count)
            else:
                return r
    except Exception as e:
        print("Error (retry {}): ".format(count), e)
        count += 1
        if count < 3:
            time.sleep(random.randint(3, 6))
            return make_request(url, count)
        else:
            return None


def get_user_comments(user_id, output_dir):
    offset = 0
    limit = 100
    url = USER_URL.format(user_id, offset, limit)
    r = make_request(url)
    if r.status_code != 200:
        return
    data = r.json()
    df = pd.DataFrame(data)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    df.to_csv("{}/{}.csv".format(output_dir, user_id), index=False)
    
todo = set()

# create a worker take user_id from todo and get all comments
def worker(worker_name, output_dir):
    print('Start worker: ', worker_name)
    count = 0
    while True:
        if len(todo) == 0:
            break
        user_id = todo.pop()
        # check save file exist then skip
        if os.path.exists("{}/{}.csv".format(output_dir, user_id)):
            continue
        count += 1
        get_user_comments(user_id, output_dir)
        if count % 100 == 0:
            print("Processed {}: {}/{}".format(worker_name, count, len(todo)))

if __name__ == "__main__":
    user_file = "checkpoints/users_1668847188.txt"
    users = open(user_file, "r").read().splitlines()
    print("Length of users: ", len(users))
    output_dir = "user_comments"
    todo = set(users)
    # Create 5 threads
    threads = []
    for i in range(5):
        t = threading.Thread(target=worker, args=("worker_{}".format(i), output_dir))
        threads.append(t)
        t.start()
    
    # Wait for all threads to finish
    for t in threads:
        t.join()