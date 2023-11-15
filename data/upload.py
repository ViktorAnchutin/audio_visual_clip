import boto3
import logging
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(
    level=logging.ERROR, 
    format='%(asctime)s:%(process)d:%(levelname)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(), logging.FileHandler('logs/s3upload.log')]
)


bucket = 'audioset-data'

session = boto3.session.Session()
s3_client = session.client("s3")

def upload_audio(key):
    try:
       s3_client.upload_file(f'audio/{key}', bucket, key)
    except Exception as e:
        logging.error(f'{key} failed: {e}')

def upload_img(key):
    try:
       s3_client.upload_file(f'imgs/{key}', bucket, key)
    except Exception as e:
        logging.error(f'{key} failed: {e}')



audios = os.listdir('audio')

with ThreadPoolExecutor() as pool:
    for _ in tqdm(pool.map(upload_audio,audios), total=len(audios)):
        pass


imgs = os.listdir('imgs')

with ThreadPoolExecutor() as pool:
    for _ in tqdm(pool.map(upload_img,imgs), total=len(imgs)):
        pass
