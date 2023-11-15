import logging
from pytube import YouTube
from moviepy.editor import VideoFileClip
from PIL import Image
from concurrent.futures import ThreadPoolExecutor
import os
from tqdm import tqdm

class DataExtractor:
    def __init__(self,audio_path, img_path, data, vgg=False, logger = None):
        self.df = data
        self.audio_path = audio_path
        self.img_path = img_path
        self.vgg = vgg
        self.log = logging.getLogger() if logger is None else logger
        
    def downloadVideo(self,video_id):
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        stream = YouTube(video_url).streams.get_highest_resolution()
        stream.download(filename=f'{video_id}.mp4')
        
    def extract_data_pair(self,video_id,segments):
        self.log.debug(f'Opening videofile {video_id}')
        clip = VideoFileClip(f'{video_id}.mp4')
        for start,end in segments:
            subclip = clip.subclip(start, end)
            audio = subclip.audio
            frame = subclip.get_frame((end-start)/2)
            frame = Image.fromarray(frame)
            segment_id = f'{video_id}_{start}_{end}'
            self.log.debug(f'Writing audio and imae to disk for {video_id} segment')
            audio.write_audiofile(f'{self.audio_path}/{segment_id}.wav',verbose=False, logger=None)
            frame.save(f'{self.img_path}/{segment_id}.png')
            self.log.debug(f'Done saving data for {video_id} segment')
        self.log.debug(f'Saved all data for {video_id} segment')
        clip.close()
    
    def remove_video(self,video_id):
        try:
            path = f'{video_id}.mp4'
            if os.path.exists(path):
                os.remove(path)
        except Exception as err:
            self.log.error(f'Failed to remove file: {video_id}')


    def extract_data_from_video(self,video_id):
            try:
                self.log.debug(f'Downloading {video_id}')
                self.downloadVideo(video_id)
                self.log.debug(f'Finished downloading {video_id}')
                sts = self.df[self.df.segment_id==video_id].start_time_seconds.values
                if self.vgg: # vgg dataset only provides start time
                    ets = sts + 10
                else:
                    ets = self.df[self.df.segment_id==video_id].end_time_seconds.values
                segments = list(zip(sts,ets))
                self.extract_data_pair(video_id,segments)
            except Exception as err:
                self.log.error(f'{video_id} FAILED : {err}')
            finally:
                self.remove_video(video_id)
    
        
    def extract_data(self,videos):
        with ThreadPoolExecutor() as pool:
            pool.map(self.extract_data_from_video,videos)
            