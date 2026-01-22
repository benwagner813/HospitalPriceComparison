import os
import zipfile
import requests
import threading
import re
import mimetypes
import time
from pathlib import Path
from queue import Queue
from Read_Hospital_CSV import HospitalChargeETLCSV
from Read_Hospital_JSON import HospitalChargeETLJSON
from urllib.parse import urlparse, unquote

def get_filename_from_url(response, original_url=None):
    """
    Extract filename from URL or Content-Disposition header.
    
    Args:
        url: The URL being downloaded
        response: Optional requests Response object to check headers
    
    Returns:
        Filename string
    """
    cd = response.headers.get("Content-Disposition")
    if cd:
        # RFC 5987: filename*=UTF-8''encoded.ext
        match = re.search(r"filename\*\s*=\s*([^']*)''(.+)", cd, re.IGNORECASE)
        if match:
            return unquote(match.group(2))

        # Standard filename="file.ext"
        match = re.search(r'filename\s*=\s*"?([^";]+)"?', cd, re.IGNORECASE)
        if match:
            return match.group(1)
   
    parsed = urlparse(response.url)
    filename = os.path.basename(unquote(parsed.path))

    if filename:
        return filename

    content_type = response.headers.get("Content-Type", "").split(";")[0]
    ext = mimetypes.guess_extension(content_type)

    if ext:
        return f"download{ext}"

    base = f"download_{hash(original_url or response.url) % 100000}"
    return base + ".bin"


def download_file(url, download_dir):
    """
    Download a file from a URL, auto-detecting the filename.
    
    Args:
        url: The URL to download from
        download_dir: Directory to save the file in
    
    Returns:
        Path to the downloaded file
    """
    print(f"Downloading from {url}...")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    try:
        response = requests.get(url, headers=headers, allow_redirects=True, stream=True)
        response.raise_for_status()
        
        # Get filename from response or URL
        filename = get_filename_from_url(response, url)
        download_path = os.path.join(download_dir, filename)
        
        with open(download_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"Downloaded to {download_path}")
        return download_path
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"Access forbidden (403) for {url}. Skipping this file...")
            return None
        else:
            raise
    except Exception as e:
        print(f"Error downloading {url}: {str(e)}")
        return None


def unzip_if_needed(file_path, extract_to=None, target_extensions=None):
    """
    Unzip file if it's a zip archive and return the target file.
    
    Args:
        file_path: Path to the file
        extract_to: Directory to extract to (default: same directory as zip)
        target_extensions: List of file extensions to look for (e.g., ['.csv', '.txt'])
                          If None, returns the extraction directory
    
    Returns:
        Tuple of (path_to_target_file, list_of_files_to_cleanup)
    """
    if not zipfile.is_zipfile(file_path):
        print(f"{file_path} is not a zip file, skipping extraction")
        return file_path, [file_path]
    
    if extract_to is None:
        extract_to = os.path.dirname(file_path)
    
    print(f"Extracting {file_path}...")
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
        extracted_files = zip_ref.namelist()
    
    print(f"Extracted {len(extracted_files)} file(s)")
    
    # Build full paths for extracted files
    extracted_paths = [os.path.join(extract_to, f) for f in extracted_files]
    
    # Always include the original zip in cleanup
    cleanup_paths = [file_path]
    cleanup_paths.extend(extracted_paths)

    # If looking for specific extensions, filter for those
    if target_extensions:
        target_files = [
            p for p in extracted_paths 
            if os.path.isfile(p) and any(p.lower().endswith(ext.lower()) for ext in target_extensions)
        ]
        
        if not target_files:
            raise ValueError(f"No files with extensions {target_extensions} found in zip")
        
        
        return target_files[0], cleanup_paths
    
    return extracted_paths[0], cleanup_paths


def process_file(file_path):
    """Move through directories and process files"""
    
    # Handle directory case (multiple files extracted)
    if os.path.isdir(file_path):
        print(f"Processing directory {file_path}...")
        # Process all CSV/JSON files in the directory
        for root, dirs, files in os.walk(file_path):
            for file in files:
                full_path = os.path.join(root, file)
                file_extension = os.path.splitext(full_path)[1].lower()
                if file_extension in ['.csv', '.json']:
                    process_single_file(full_path)
        return
    
    # Handle single file case
    process_single_file(file_path)

def process_single_file(file_path):
    """Process a single file"""
    print(f"Processing {file_path}...")
    
    db_connection_str = ""
    with open("../Credentials/cred.txt", "r") as f:
        db_connection_str = f.readline()

    file_extension = os.path.splitext(file_path)[1].lower()

    if file_extension == '.json':
        etl = HospitalChargeETLJSON(db_connection_str, file_path)
        result = etl.execute()
    elif file_extension == '.csv':
        etl = HospitalChargeETLCSV(db_connection_str, file_path)
        result = etl.execute()
    else:
        raise ValueError(f"Unsupported file type: {file_extension}. Only .json and .csv are supported.")

    print(f"Processing complete: {file_path}")


def cleanup(paths):
    """Delete files and directories."""
    import shutil
    for path in paths:
        try:
            if os.path.isfile(path):
                os.remove(path)
                print(f"Deleted file: {path}")
            elif os.path.isdir(path):
                shutil.rmtree(path)
                print(f"Deleted directory: {path}")
        except Exception as e:
            print(f"Warning: Could not delete {path}: {e}")


def download_worker(url_queue, result_queue, download_dir, target_extensions=None):
    """
    Worker thread that downloads files.
    
    Args:
        url_queue: Queue of URLs to download
        result_queue: Queue to put results in
        download_dir: Directory to download to
        target_extensions: List of file extensions to extract from zips (e.g., ['.csv', '.json'])
    """
    while True:
        item = url_queue.get()
        if item is None:  # Poison pill to stop thread
            break
        
        url = item
        extracted_path = None
        cleanup_paths = []
        
        try:
            downloaded_file = download_file(url, download_dir)
            extracted_path, cleanup_paths = unzip_if_needed(
                downloaded_file, 
                target_extensions=target_extensions
            )
            result_queue.put(('success', extracted_path, cleanup_paths))
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            # Even on error, try to cleanup what we downloaded
            if extracted_path:
                cleanup_paths.append(extracted_path)
            result_queue.put(('error', None, cleanup_paths))
        finally:
            url_queue.task_done()


def pipeline_process(urls, download_dir="./downloads", max_buffered=1, target_extensions=None):
    """
    Download and process files with pipelining and backpressure control.
    Downloads next file while processing current file, but limits how many
    files can be downloaded ahead to prevent running out of disk space.
    
    Args:
        urls: List of URL strings to download
        download_dir: Directory to download files to
        max_buffered: Max number of files to download ahead (default: 1)
        target_extensions: List of file extensions to extract from zips (e.g., ['.csv', '.json'])
                          If None, extracts all files
    """
    os.makedirs(download_dir, exist_ok=True)
    
    # Use maxsize to limit queue - this provides backpressure!
    url_queue = Queue(maxsize=max_buffered)
    result_queue = Queue(maxsize=max_buffered)
    
    # Start download worker thread
    download_thread = threading.Thread(
        target=download_worker,
        args=(url_queue, result_queue, download_dir, target_extensions)
    )
    download_thread.start()
    
    # Feed URLs in a separate thread to avoid blocking
    def feed_urls():
        for url in urls:
            url_queue.put(url)  # Blocks if queue is full (backpressure!)
        url_queue.put(None)  # Poison pill
    
    feeder_thread = threading.Thread(target=feed_urls)
    feeder_thread.start()
    
    # Process files as they become available
    for i in range(len(urls)):
        # Wait for current file to finish downloading
        status, file_path, cleanup_paths = result_queue.get()
        
        if status == 'success':
            try:
                # Process the file
                process_file(file_path)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
            finally:
                # Cleanup - cleanup_paths already contains everything that needs to be deleted
                # including the zip file and extracted files
                cleanup(cleanup_paths)
        else:
            # Error case - still cleanup what we can
            cleanup(cleanup_paths)
        
    # Wait for threads to finish
    feeder_thread.join()
    download_thread.join()

def main():
    # Configuration - just list URLs, fil enames are auto-detected
    overall_start = time.time()

    urls = set()
    file_name = "cms-hpt.txt"
    download_dir = "./downloads"
    hospital_list = [
        "https://acrmc.com/wp-content/uploads/2025/03/cms-hpt.txt",
        "https://res.cloudinary.com/dpmykpsih/raw/upload/acmc-site-340/media/r/bf353e0b1e8e45c39fcbd6537ba77aae/cms-hpt.txt",
        "https://estimator.myinsightcare.com/cms-hpt.txt",
        "https://www.nationwidechildrens.org/cms-hpt.txt",
        "https://www.ovsurgical.com/cms-hpt.txt",
        "https://trinityhealth.com/cms-hpt.txt",
        "https://www.waynehealthcare.org/cms-hpt.txt",
        "https://www.westernreservehospital.org/sites/default/files/cms-hpt.txt",
        "https://www.wilsonhealth.org/cms-hpt.txt",
        "https://www.woodcountyhospital.org/cms-hpt.txt",
        "https://www.woosterhospital.org/cms-hpt.txt",
        "https://www.wyandotmemorial.org/cms-hpt.txt",
        "https://wvumedicine.org/cms-hpt.txt",
        "https://www.uchealth.com/cms-hpt.txt",
        "https://www.mountcarmelhealth.com/sites/default/files/cms-hpt.txt",
        "https://www.mhsystem.org/cms-hpt.txt",
        "https://www.holzer.org/cms-hpt.txt",
        "https://www.uhhospitals.org/cms-hpt.txt",
        "https://www.firelands.com/cms-hpt.txt",
        "https://childrensdayton.org/cms-hpt.txt",
        "https://www.cincinnatichildrens.org/cms-hpt.txt",
        "https://www.akronchildrens.org/cms-hpt.txt",
        # "https://www.crystalclinic.com/cms-hpt.txt",
        # "https://www.fmchealth.org/cms-hpt.txt",
        # "https://www.genesishcs.org/cms-hpt.txt",
        # "https://www.henrycountyhospital.org/cms-hpt.txt",
        # "https://www.hdh.org/cms-hpt.txt",
        # "https://www.hvch.org/cms-hpt.txt",
        # "https://insightchicago.com/cms-hpt.txt",
        # "https://www.ioshospital.com/cms-hpt.txt",
        # "https://www.kingsdaughtershealth.com/cms-hpt.txt",
        # "https://www.lmhealth.org/cms-hpt.txt",
        # "https://www.madison-health.com/cms-hpt.txt",
        # "https://www.magruderhospital.com/cms-hpt.txt",
        # "https://memorialohio.com/cms-hpt.txt",
        # "https://www.metrohealth.org/cms-hpt.txt",
        # "https://pauldingcountyhospital.com/cms-hpt.txt",
        # "https://www.pomerenehospital.org/cms-hpt.txt",
        # "https://cdn.prod.website-files.com/5ad49dbf3b9e2b3b0ba15f49/695bc312004b2d2b3df8afc8_cms-hpt.txt",
        # "https://www.somc.org/cms-hpt.txt",
        # "https://www.swgeneral.com/cms-hpt.txt",
        # "https://www.southwoodshealth.com/cms-hpt.txt",
        # "https://www.thechristhospital.com/cms-hpt.txt",
        # "https://health.utoledo.edu/cms-hpt.txt",
        # "https://www.trihealth.com/cms-hpt.txt",
        # "https://coshoctonhospital.org/cms-hpt.txt",
        # "https://elch.org/cms-hpt.txt",
        # "https://fultoncountyhealthcenter.org/cms-hpt.txt",
        # "https://www.kch.org/cms-hpt.txt",
        # "https://www.limamemorial.org/cms-hpt.txt",
        # "https://res.cloudinary.com/dpmykpsih/raw/upload/mary-rutan-redesign-site-505/media/r/45d4f28cfbc64b8a92d2c639fa6299d1/cms-hpt.txt",
        # "https://mercer-health.com/cms-hpt.txt",
        # "https://www.parkview.com/cms-hpt.txt",
        # "https://www.summahealth.org/cms-hpt.txt",
        # "https://www.trinitytwincity.org/cms-hpt.txt",
        # "https://my.clevelandclinic.org/cms-hpt.txt",
        # "https://www.mercy.com/-/media/mercy/cms-hpt.txt",
        # "https://ketteringhealth.org/cms-hpt.txt",
        # "https://www.ohiohealth.com/cms-hpt.txt",
        # "https://www.adena.org/cms-hpt.txt",
        # "https://aultman.org/cms-hpt.txt",
        # "https://www.bvhealthsystem.org/cms-hpt.txt",
        # "https://www.uhhospitals.org/cms-hpt.txt",
        # "https://pcl.promedica.org/-/media/pay-my-bill/cms-hpt.txt",
        # "https://www.premierhealth.com/cms-hpt.txt",
        # "https://avitahealth.org/cms-hpt.txt",
        # "https://wexnermedical.osu.edu/cms-hpt.txt",
    ]

    for website in hospital_list:
        file_name = download_file(website, download_dir)
        if file_name is not None:
            with open(file_name, "r") as f:
                for line in f:
                    if "location-name" in line:
                        print(f"Adding MRF for {line[line.find(":") + 1:].strip()} to the list of urls")
                    if "mrf-url" in line:
                        urls.add(line[line.find(":") + 1:].strip())

    file_name = download_file("https://urldefense.com/v3/__https://hospitalpricedisclosure.com/download.aspx?pi=WJKzY1WfxL9tY*__*rvbuKxvw*-*__;KioqKg!!P9Xnu6eaQvFHhbdAE0-F5A!saewjBGHS4GsuJknkffuFQqKgq-h88YcMSevwk0zZkROIZEATF3T3ig7BWKG-ZecarTsl9EC8hwW3_cV6p-_51OodIg$", download_dir)
    print(f"Found file: {file_name}")
    # Optional: specify which file types to extract from zips   
    # This will ignore readme files, metadata, etc.
    target_extensions = ['.csv', '.json']  # Or None to extract everything
    num_urls = len(urls)
    print("=" * 50)
    print(f"Found {num_urls} MRFs, beginning processing")
    print("=" * 50)
    # max_buffered=1 means download at most 1 file ahead
    # Increase if you have more disk space and want more parallelism
    pipeline_process(urls, download_dir, max_buffered=3, target_extensions=target_extensions)
    total_time = time.time() - overall_start

    print(f"The process took {total_time:.2f}s")


if __name__ == "__main__":
    main()