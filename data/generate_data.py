from data_extractor import DataExtractor
import logging
import pandas as pd
import os
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
from functools import partial
import re 


logging.basicConfig(
    level=logging.ERROR, 
    format='%(asctime)s:%(process)d:%(levelname)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

audiodir = 'audio'
imdir = 'imgs'
os.makedirs(audiodir, exist_ok=True)
os.makedirs(imdir, exist_ok=True)

train_df = pd.read_csv('datasets/animals_dataset_train.csv', index_col=0)
eval_df = pd.read_csv('datasets/animals_dataset_eval.csv', index_col=0)
data = pd.concat([train_df,eval_df])


# filter out already processed videos

pattern = rf"(.*?)_\d+\.\d+_\d+\.\d+\.png$" 

def extract_video_id(img_name):
    return re.findall(pattern, img_name)[0]

processed_videos = [extract_video_id(img) for img in os.listdir('imgs')]
print(f"removing {data.segment_id.isin(processed_videos).sum()} items")
data = data[~data.segment_id.isin(processed_videos)]


def run_task(videos:list):
    logger = logging.getLogger('task_logger')
    handler = logging.FileHandler(f'logs/logfile_{os.getpid()}.log')
    handler.setFormatter(logging.root.handlers[0].formatter)
    logger.addHandler(handler) # separate log file for each process
    #logger.setLevel(logging.ERROR)
    data_extractor = DataExtractor(audiodir, imdir, data,logger=logger)
    data_extractor.extract_data(videos)
    
chunk_size = 5
videos = list(data.segment_id.unique())
videos_chunks = [videos[i:i+chunk_size] for i in range(0,len(videos),chunk_size)] 

with ProcessPoolExecutor() as pool:
    for _ in tqdm(pool.map(run_task,videos_chunks), total=len(videos_chunks)):
        pass
#list(map(task_runner,videos_chunks))
