import requests
from bs4 import BeautifulSoup
import re
import time
import random
import os
import threading
init_urls = [
    "https://vnexpress.net/",
    "https://vnexpress.net/elon-musk-sa-thai-nhan-vien-sua-ong-tren-twitter-4536099.html"
]
todo = set(init_urls)
visited = set()
users = set()

current_user_size = -1

POST_COMMENT_URL = "https://usi-saas.vnexpress.net/index/get?offset={}&limit=25&frommobile=1&sort=like&is_onload=0&objectid={}&objecttype=1&siteid={}"


def check_valid_url(url):
    # Check if the url is valid
    if url.startswith("https://vnexpress.net/"):
        return True
    return False

def is_post_url(url):
    # Check if the url is a post url
    if check_valid_url(url) and re.match(r".+-\d+\.html", url):
        if len(url.split('/')) == 4:
            return True
    return False


def prep_url(url):
    if url.startswith("/"):
        url = "https://vnexpress.net" + url
    # remove param from url
    if "?" in url:
        url = url.split("?")[0]
    if '#' in url:
        url = url.split("#")[0]
    return url

# Create a function to find all the links in a html content and add to todo
def find_links(html):
    soup = BeautifulSoup(html, "html.parser")
    links = soup.find_all("a", href=True)
    for link in links:
        href = prep_url(link.get("href"))
        if is_post_url(href) and href not in todo and href not in visited:
            todo.add(href)


def make_request(url, count=0):
    time.sleep(0.5)
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


# get all users in a post comment
def get_user_in_comment(post_id, site_id):
    offset = 0
    while True:
        url = POST_COMMENT_URL.format(offset, post_id, site_id)
        r = make_request(url)
        if r.status_code != 200:
            return
        data = r.json()
        if not data["data"]:
            return
        total = int(data['data'].get('total', 0))
        if offset >= total:
            return
        for item in data["data"]["items"]:
            userid = item["userid"]
            if userid:
                users.add(userid)
        offset += 25

def save_checkpoint(limit=5):
    global current_user_size
    save_dir = "checkpoints"
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    # count number of file in save_dir
    files = os.listdir(save_dir)
    if len(files) >= limit:
        # remove oldest file
        oldest_file = min(files, key=lambda x: os.path.getctime(os.path.join(save_dir, x)))
        os.remove(os.path.join(save_dir, oldest_file))
    if len(users) <= current_user_size:
        print('Skip save checkpoint because users size is {} and current_user_size is {}'.format(len(users), current_user_size))
        return
    current_user_size = len(users)
    # save users
    with open(os.path.join(save_dir, "users_{}.txt".format(int(time.time()))), "w") as f:
        for user in users:
            f.write(str(user) + "\n")


def get_infos_from_url(url):
    r = make_request(url)
    if r.status_code != 200:
        return
    html = r.text
    soup = BeautifulSoup(html, "html.parser")
    try:
        objectid = re.sub(r'.+\-(\d+)\.html', r'\1', url)
        # get siteid from meta tag name tt_site_id
        siteid = soup.find("meta", {"name": "tt_site_id"}).get("content")
    except:
        print("Can not get objectid or siteid from url: ", url)
        return None, None
    return objectid, siteid



# Create a worker function take url from todo and add to visited
def worker(worker_name):
    count = 0
    while True:
        count += 1
        if not todo:
            break
        url = todo.pop()
        visited.add(url)
        r = make_request(url)
        if not r or r.status_code != 200:
            continue
        html = r.text
        find_links(html)
        if is_post_url(url):
            objectid, siteid = get_infos_from_url(url)
            if objectid and siteid:
                get_user_in_comment(objectid, siteid)
        if count % 100 == 0:
            print("Worker {} - Visited: {}, Todo: {}, Users: {}".format(worker_name, len(visited), len(todo), len(users)))
            t = threading.Thread(target=save_checkpoint)
            t.start()
            count = 0


if __name__ == '__main__':
    # r = make_request("https://vnexpress.net/elon-musk-sa-thai-nhan-vien-sua-ong-tren-twitter-4536099.html")
    # find_links(r.text)
    # print(todo)
    # exit(0)
    # Create a 5 thread to run worker function
    
    for i in range(5):
        t = threading.Thread(target=worker, args=(i,))
        t.start()
