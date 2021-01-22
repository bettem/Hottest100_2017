import os
from multiprocessing import Pool, cpu_count
from time import sleep

import pytesseract
from PIL import Image
from fuzzywuzzy import fuzz
from tqdm import tqdm

import database


def ocr_image(file, image_directory):
    try:
        pytesseract.pytesseract.tesseract_cmd = 'C:\\Program Files (x86)\\Tesseract-OCR\\tesseract'
        valid_image = False
        filename = os.fsdecode(file)
        cxn = database.connection()
        cxn.connect()
        # If the file is a JPG
        if filename.endswith(".jpg"):
            img = Image.open(os.path.join(image_directory, file))
            text = pytesseract.image_to_string(img, lang='eng')
            # iterate over the OCR results, cleaning strings and storing the results
            for line in text.splitlines():
                if valid_image == True:
                    cxn.insert_vote_results(filename[:-4], line)
                if fuzz.ratio(line, 'Your Hottest 100 Votes:') >= 90:
                    valid_image = True
            cxn.insert_processed_image(filename.split('.')[0])
        # Disconnect from Datastore
        cxn.disconnect()
    except Exception as e:
        print(e)


def process_images(image_directory, processed_images):
    # Variables & connect to data store.
    cxn = database.connection()
    cxn.connect()
    images_to_process = []

    # Build a queue of images to process
    print(len(processed_images))
    for file in os.listdir(image_directory):
        if os.fsdecode(file).split('.')[0] not in processed_images:
            images_to_process.append(file)

    # Initialise asynchronous OCR
    p = Pool(cpu_count())
    jobs = []
    task_count = len(images_to_process)
    completed_count = 0
    print('Commencing OCR on posts.')
    for image in images_to_process:
        jobs.append(p.apply_async(ocr_image, args=(image, image_directory)))
    pbar = tqdm(total=task_count, unit="images")
    # Processing Progress
    while True:
        incomplete_count = sum(1 for x in jobs if not x.ready())
        pbar.update(abs(completed_count - (task_count - incomplete_count)))
        completed_count = task_count - incomplete_count
        if sum(1 for x in jobs if not x.ready()) == 0:
            break
        sleep(0.1)

    # Disconnect from Datastore
    cxn.disconnect()
