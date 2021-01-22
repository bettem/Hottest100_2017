import json
import sys
from datetime import datetime
from multiprocessing import cpu_count, Pool
from time import sleep

import tqdm
import urllib3
from bs4 import BeautifulSoup


def process_post(post, profile):
    # Initialise the urllib components
    http = urllib3.PoolManager()
    urllib3.disable_warnings()

    process_post = True
    try:
        post_id = post['node']['id']  # ID for Filenames etc.
        post_is_video = post['node']['is_video']  # Is the post a Video
        file_url = post['node']['thumbnail_src']  # Source location
        post_datetime = post['node']['taken_at_timestamp']  # Post datetimestmap
    except KeyError:
        print("Error extracting Post Data")
        raise

    # If the post is not in the specified time range, skip this post
    if profile['start_datetime'] > post_datetime or post_datetime > profile['end_datetime']:
        process_post = False

    # If the post is a video and videos were not specified, skip the post
    if profile['get_videos'] == False and post_is_video == True:
        process_post = False

    # If it was specified as Videos Only and the image is a picture, skip the post
    if profile['get_videos_only'] == True and post_is_video == False:
        process_post = False

    # If the post falls within all criteria specified in the profile, commence processing
    if process_post == True:
        # Save the Post JSON if required
        if profile['get_post_json'] == True:
            with open(profile['directory'] + str(post_id) + '.json', 'w') as outfile:
                json.dump(post, outfile)

        # Save the image/video to the directory
        if profile['get_post_json_only'] == False:
            file_extension = file_url.split('/')[-1].split('.')[-1]
            try:
                req = http.request('GET', file_url, preload_content=False)
                with open(profile['directory'] + str(post_id) + '.' + file_extension, 'wb') as outfile:
                    while True:
                        data = req.read(64)
                        if not data:
                            break
                        outfile.write(data)
            except Exception as e:
                print("Error Downloading Post(" + str(post_id) + ": ", sys.exc_info()[0])
                raise
            req.release_conn()


class Scraper:
    def __init__(self, profile):
        self.profile = profile
        self.profile['start_datetime'] = datetime.timestamp(profile['start_datetime'])
        self.profile['end_datetime'] = datetime.timestamp(profile['end_datetime'])
        self.process_queue = []
        self.http = urllib3.PoolManager()
        # Disable SSL Warnings
        urllib3.disable_warnings()

    def fetch_page(self, url):
        # Attempt to fetch the page
        data = None
        try:
            req = self.http.request('GET', url)
        except Exception as e:
            print("Page Connection Error: ", sys.exc_info()[0])
            raise
        # Strip the JSON contents from the page
        try:
            soup = BeautifulSoup(req.data, "lxml")
            scripts = soup.findAll('script')
            for script in scripts:
                if 'window._sharedData = ' in script.string:
                    data = json.loads(script.string[21:-1])
                    break
        except Exception as e:
            print("Error accessing page payload: ", sys.exc_info()[0])
            raise
        return data

    def process_post_queue(self):
        # Download the posts queued asynchronously
        p = Pool(cpu_count())
        jobs = []
        task_count = len(self.process_queue)
        completed_count = 0
        print('Commencing to scrape posts.')
        for post in self.process_queue:
            jobs.append(p.apply_async(process_post, args=(post, self.profile)))
        pbar = tqdm.tqdm(total=task_count, unit="posts")
        # Processing Progress
        while True:
            incomplete_count = sum(1 for x in jobs if not x.ready())
            pbar.update(abs(completed_count - (task_count - incomplete_count)))
            completed_count = task_count - incomplete_count

            if sum(1 for x in jobs if not x.ready()) == 0:
                print('Post scraping complete.')
                break
            sleep(0.1)

    def process_page(self, url):
        page_data = {}
        page_json = self.fetch_page(url)
        try:
            page_data['has_next_page'] = \
                page_json['entry_data']['TagPage'][0]['graphql']['hashtag']['edge_hashtag_to_media']['page_info'][
                    'has_next_page']
            page_data['next_page'] = \
                page_json['entry_data']['TagPage'][0]['graphql']['hashtag']['edge_hashtag_to_media']['page_info'][
                    'end_cursor']
            for post in page_json['entry_data']['TagPage'][0]['graphql']['hashtag']['edge_hashtag_to_media']['edges']:
                self.process_queue.append(post)
            return page_data
        except KeyError:
            print('KeyError: Invalid JSON Key, revisit document')
            raise

    def download(self):
        # Iterate over each tag to download
        for hashtag in self.profile['hashtags']:
            # Download the initial page
            print('Scraping pages with #' + hashtag)
            url = 'https://www.instagram.com/explore/tags/' + hashtag + '/'
            page = self.process_page(url)
            # Iterate over subsequent pages
            while page['has_next_page'] == True:
                url = 'https://www.instagram.com/explore/tags/' + hashtag + '/?max_id=' + page['next_page']
                page = self.process_page(url)
                try:
                    # Only check the last post on the page to see if its outside of the timerange
                    print('Last post on page posted at: ' + str(
                        datetime.fromtimestamp(self.process_queue[-1]['node']['taken_at_timestamp'])))
                    if self.process_queue[-1]['node']['taken_at_timestamp'] < self.profile['start_datetime']:
                        break
                except KeyError:
                    print('KeyError: Unable to determine post timestamps')
                    raise
        print('Total Potential Posts: ' + str(len(self.process_queue)))

        self.process_post_queue()
