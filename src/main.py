from typing import List
import os
import csv
import asyncio
from download_utils.download import download_async
from wet_mining_utils.duck_based import ArchiveExtractor
from tkinter import filedialog

HOSTNAME = 'https://data.commoncrawl.org/'

async def download_worker(paths: List[str], queue: asyncio.Queue):
    for path in paths:
        try:
            data = await download_async(path)
            await queue.put(data)
        except Exception as e:
            print(f"Error downloading {path}: {e}")
    
    # 終了を通知
    await queue.put(None)

async def process_worker(queue: asyncio.Queue, keywords: List[str], export_dir: str):
    with ArchiveExtractor(keywords) as extractor:
        exported_files = 0

        while True:
            data = await queue.get()
            
            # None を受け取ったら終了
            if data is None:
                queue.task_done()
                break
                
            try:
                extractor.load_data(data)
                results = extractor.extract()
                # ここでresultsを使った処理を行う
                if len(results) > 0:
                    with open(export_dir + f'/cc-wet-extracted-{str(exported_files + 1)}.csv', 'w', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerows([[x.timestamp, x.text] for x in results])
                    
                extractor.reset()
            except Exception as e:
                print(f"Error processing data: {e}")
            finally:
                queue.task_done()

async def main():
    with open(os.path.dirname(__file__) + r'\dependencies\wet.paths', 'r', encoding='utf-8') as f:
        paths = [HOSTNAME + l.rstrip('\n') for l in f.readlines()]

    with open(os.path.dirname(__file__) + r'\dependencies\keywords.txt', 'r', encoding='utf-8') as f:
        keywords = [l.rstrip('\n') for l in f.readlines()]

    # 出力ディレクトリの選択
    export_dir = filedialog.askdirectory()

    # キューの作成
    queue = asyncio.Queue(4)
    
    # ダウンロードワーカーとプロセスワーカーの作成
    download_task = asyncio.create_task(download_worker(paths, queue))
    process_task = asyncio.create_task(process_worker(queue, keywords, export_dir))
    
    # 全てのタスクが完了するまで待機
    await asyncio.gather(download_task, process_task)

if __name__ == '__main__':
    asyncio.run(main())