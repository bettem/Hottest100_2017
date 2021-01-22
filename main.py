from datetime import datetime, timedelta

from fuzzywuzzy import process

import InstagramScraper
import OCR
import database

if __name__ == '__main__':
    # Define the Download Profile
    profile = {
        "directory": "C:\\Users\\bett3\\Desktop\\Projects\\Hottest100\\InstagramData\\"
        , "hashtags": ['hottest100', 'triplej']
        , "start_datetime": datetime.now() - timedelta(days=15)
        , "end_datetime": datetime.now()
        , "get_videos": False
        , "get_videos_only": False
        , "get_post_json": True
        , "get_post_json_only": False
    }

    # Execute the Downloader
    IL = InstagramScraper.Scraper(profile)
    IL.download()

    # Run the OCR
    cxn = database.connection()
    cxn.connect()
    processed_images = cxn.get_processed_votes()
    OCR.process_images(profile['directory'], processed_images)

    # Retrieve the results from OCR, match to valid songs
    song_list = cxn.get_song_list()
    votes = cxn.get_raw_votes()
    for vote in votes:
        Post_ID = vote[0]
        OCR_Artist_Track_Name = vote[1]
        Match = process.extractOne(OCR_Artist_Track_Name, song_list)
        Match_Artist_Track_Name = Match[0]
        Match_Likelihood = Match[1]
        cxn.insert_match_results(Post_ID, OCR_Artist_Track_Name, Match_Artist_Track_Name, Match_Likelihood)
    cxn.set_vote_processed()
    cxn.disconnect()
    print('Finished at ' + str(datetime.now()))
