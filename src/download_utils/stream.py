from typing import *
import aiohttp
from tqdm import tqdm

async def stream_async(url:str, chunk_size:int):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            with tqdm(
                desc = 'downloading...',
                total = int(response.headers.get('content-length', 0))
            ) as progress:
                async for chunk in response.content.iter_chunked(chunk_size):
                    yield chunk
                    progress.update(len(chunk))