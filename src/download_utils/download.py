import aiohttp
from tqdm import tqdm
from io import BytesIO
import asyncio
from aiohttp import ClientTimeout

async def download_async(
    url: str, 
    chunk_size: int = 8192,
    timeout: int = 30,
    max_retries: int = 3
) -> bytes:
    
    retry_count = 0
    timeout_settings = ClientTimeout(total=timeout)

    while retry_count < max_retries:
        try:
            with BytesIO() as buf:
                async with aiohttp.ClientSession(timeout=timeout_settings) as session:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        content_length = int(response.headers.get('content-length', 0))
                        
                        with tqdm(
                            desc='Downloading',
                            total=content_length,
                            unit='B',
                            unit_scale=True
                        ) as progress:
                            async for chunk in response.content.iter_chunked(chunk_size):
                                progress.update(buf.write(chunk))

                buf.seek(0)
                return buf.read()

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            retry_count += 1
            if retry_count >= max_retries:
                raise
            await asyncio.sleep(1 * retry_count)  # 指数バックオフ

    raise RuntimeError("Maximum retries exceeded")  # 通常ここには到達しない