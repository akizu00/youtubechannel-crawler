from __future__ import annotations
import yt_dlp
import os
from tqdm import tqdm
from typing import Callable, Any
import queue
import multiprocess as mp
import sys
from pydantic import BaseModel
from data_util.util import get_debug_flag
from data_util.util import lock_file
import pydantic
import random
import time

class Logger:
    def debug(self, msg: str):
        if get_debug_flag():
            print(msg)
    def warning(self, msg: str):
        if get_debug_flag():
            print(msg)
    def error(self, msg: str):
        print(msg, file=sys.stderr)

default_logger = Logger()

def worker_process(input_queue: mp.Queue, output_queue: mp.Queue, worker_id: int):
    """Worker process that processes playlist URLs from input queue"""
    while True:
        try:
            # Get work item with timeout
            playlist_url = input_queue.get(timeout=1)
            if playlist_url is None:  # Poison pill to stop worker
                break
                
            # Process the playlist
            result = get_metadata_from_playlist_url(playlist_url)
            output_queue.put((playlist_url, result))
            
        except queue.Empty:
            continue
        except Exception as e:
            default_logger.error(f"Worker {worker_id} error: {e}")
            output_queue.put((playlist_url, None))

def get_metadata_from_playlist_url(playlist_url: str) -> dict[str, str] | None:
    video_id_to_video_url: dict[str, str] = {}
    ydl_opts = {
        "geo_bypass": True,
        "logger": default_logger,
        "verbose": get_debug_flag(),
        "ignoreerrors": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            info = ydl.extract_info(playlist_url, process=False, download=False)
        except Exception as e:
            default_logger.error(f"Failed to extract info from {playlist_url}: {e}")
            return None
        
        if (info is None) or ("id" not in info) or ("entries" not in info):
            return None
        
        playlist_id = info["id"]
        entries = info["entries"]
        for e in entries:
            if ("url" not in e) or ("id" not in e):
                continue
            video_id_to_video_url[e["id"]] = e["url"]
    return video_id_to_video_url

class Args(pydantic.BaseModel):
    playlist_path: str
    video_links_path: str

class FetchItem(pydantic.BaseModel):
    video_id: str
    video_url: str

if __name__ == "__main__":
    args = Args.model_validate_json(open(sys.argv[1]).read())
    
    # read fetched video ids
    fetched_ids = set()
    with lock_file(args.video_links_path):
        if os.path.exists(args.video_links_path):
            with open(args.video_links_path, "r") as f:
                for line in f:
                    item = FetchItem.model_validate_json(line)
                    fetched_ids.add(item.video_id)
    
    # get all playlist urls
    playlist_url_list = []
    for line in open(args.playlist_path):
        if line.startswith("#"):
            continue
        playlist_url = line.strip()
        playlist_url_list.append(playlist_url)
    
    # Shuffle for load balancing
    random.shuffle(playlist_url_list)
    
    # Setup multiprocessing with queues
    num_workers = 16
    input_queue = mp.Queue()
    output_queue = mp.Queue()
    
    # Start worker processes
    workers = []
    for i in range(num_workers):
        worker = mp.Process(target=worker_process, args=(input_queue, output_queue, i))
        worker.start()
        workers.append(worker)
    
    # Add all work to input queue
    for playlist_url in playlist_url_list:
        input_queue.put(playlist_url)
    
    # Add poison pills to stop workers
    for _ in range(num_workers):
        input_queue.put(None)
    
    # Collect results with progress bar
    completed = 0
    with tqdm(total=len(playlist_url_list), desc="fetching ...") as pbar:
        while completed < len(playlist_url_list):
            try:
                playlist_url, video_id_to_video_url = output_queue.get(timeout=30)
                completed += 1
                pbar.update(1)
                
                # Skip None results (failed playlist processing)
                if video_id_to_video_url is None:
                    continue
                    
                # Write results to file
                with lock_file(args.video_links_path):
                    with open(args.video_links_path, "a") as f:
                        for video_id, video_url in video_id_to_video_url.items():
                            if video_id in fetched_ids:
                                continue
                            fetched_ids.add(video_id)
                            f.write(FetchItem(
                                video_id=video_id,
                                video_url=video_url,
                            ).model_dump_json() + "\n")
                            
            except queue.Empty:
                default_logger.error("Timeout waiting for results")
                break
    
    # Wait for all workers to finish
    for worker in workers:
        worker.join()
        
    print(f"Completed processing {completed}/{len(playlist_url_list)} playlists")
