import os
import aiohttp
import asyncio
import zipfile

# List of URLs to download zip files from
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
# Asynchronously downloads a file from the given URL and saves it to the downloads_dir.
 
    filename = url.split("/")[-1]  # Extract the filename from the URL
    file_path = os.path.join(downloads_dir, filename)  # Full path to save the file
    try:
        async with session.get(url) as resp:  # Make an HTTP GET request
            resp.raise_for_status()  # Raise an error for bad status
            with open(file_path, "wb") as f:  # Open file for writing in binary mode
                while True:
                    chunk = await resp.content.read(8192)  # Read in 8KB chunks
                    if not chunk:
                        break  # Exit loop if no more data
                    f.write(chunk)  # Write chunk to file
        print(f"Downloaded {filename}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")  # Print error if download fails

async def unzip_files(downloads_dir):
# Asynchronously unzips all .zip files in the downloads_dir.
    
    loop = asyncio.get_event_loop()  # Get the current event loop
    for file in os.listdir(downloads_dir):  # Iterate over files in the directory
        if file.endswith(".zip"):  # Check if the file is a zip file
            zip_path = os.path.join(downloads_dir, file)  # Full path to the zip file
            print(f"Unzipping {file}...")
            # Run blocking zip extraction in a thread to avoid blocking the event loop
            await loop.run_in_executor(None, extract_and_delete_zip, zip_path, downloads_dir)
            print(f"Unzipped and deleted {file}")

def extract_and_delete_zip(zip_path, extract_to):
# Extracts a zip file to the specified directory and deletes the zip file.

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)  # Extract all contents
    os.remove(zip_path)  # Delete the zip file after extraction

async def main():
# Main asynchronous function to download and unzip files.

    downloads_dir = "downloads"  # Directory to save downloads
    os.makedirs(downloads_dir, exist_ok=True)  # Create directory if it doesn't exist
    async with aiohttp.ClientSession() as session:  # Create an aiohttp session
        # Create a list of download tasks for all URLs
        tasks = [download_file(session, url, downloads_dir) for url in download_uris]
        await asyncio.gather(*tasks)  # Run all download tasks concurrently
    await unzip_files(downloads_dir)  # Unzip all downloaded files
    print("All files downloaded and unzipped")

if __name__ == "__main__":
# Run the main async function if this script is executed directly
    asyncio.run(main())
