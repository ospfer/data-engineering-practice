import os
import aiohttp
import asyncio
import zipfile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

async def download_file(session, url, downloads_dir):
    filename = url.split("/")[-1]
    file_path = os.path.join(downloads_dir, filename)
    try:
        async with session.get(url) as resp:
            resp.raise_for_status()
            with open(file_path, "wb") as f:
                while True:
                    chunk = await resp.content.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
        print(f"Downloaded {filename}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")

async def unzip_files(downloads_dir):
    loop = asyncio.get_event_loop()
    for file in os.listdir(downloads_dir):
        if file.endswith(".zip"):
            zip_path = os.path.join(downloads_dir, file)
            print(f"Unzipping {file}...")
            # Run blocking zip extraction in a thread to avoid blocking the event loop
            await loop.run_in_executor(None, extract_and_delete_zip, zip_path, downloads_dir)
            print(f"Unzipped and deleted {file}")

def extract_and_delete_zip(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    os.remove(zip_path)

async def main():
    downloads_dir = "downloads"
    os.makedirs(downloads_dir, exist_ok=True)
    async with aiohttp.ClientSession() as session:
        tasks = [download_file(session, url, downloads_dir) for url in download_uris]
        await asyncio.gather(*tasks)
    await unzip_files(downloads_dir)
    print("All files downloaded and unzipped")

if __name__ == "__main__":
    asyncio.run(main())

