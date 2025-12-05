import os
import zipfile
import requests
import threading
from pathlib import Path
from queue import Queue
from Read_Hospital_CSV import HospitalChargeETLCSV


def get_filename_from_url(url, response=None):
    """
    Extract filename from URL or Content-Disposition header.
    
    Args:
        url: The URL being downloaded
        response: Optional requests Response object to check headers
    
    Returns:
        Filename string
    """
    # Try to get filename from Content-Disposition header
    if response and 'Content-Disposition' in response.headers:
        content_disp = response.headers['Content-Disposition']
        if 'filename=' in content_disp:
            filename = content_disp.split('filename=')[1].strip('"\'')
            return filename
    
    # Fall back to URL parsing
    from urllib.parse import urlparse, unquote
    parsed = urlparse(url)
    filename = os.path.basename(unquote(parsed.path))
    
    # If still no filename, generate one
    if not filename or filename == '':
        filename = f"download_{hash(url) % 100000}.bin"
    
    return filename


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
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    # Get filename from response or URL
    filename = get_filename_from_url(url, response)
    download_path = os.path.join(download_dir, filename)
    
    with open(download_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"Downloaded to {download_path}")
    return download_path


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
    
    # If looking for specific extensions, filter for those
    if target_extensions:
        target_files = [
            p for p in extracted_paths 
            if os.path.isfile(p) and any(p.lower().endswith(ext.lower()) for ext in target_extensions)
        ]
        
        if not target_files:
            raise ValueError(f"No files with extensions {target_extensions} found in zip")
        
        if len(target_files) == 1:
            # Return the target file, cleanup everything else
            cleanup_paths.extend([p for p in extracted_paths if p != target_files[0]])
            return target_files[0], cleanup_paths
        else:
            # Multiple target files - return directory, cleanup zip only
            print(f"Found {len(target_files)} target files: {target_files}")
            return extract_to, cleanup_paths
    
    # No specific extensions requested
    if len(extracted_files) == 1:
        # Single file - return it, cleanup zip
        return extracted_paths[0], cleanup_paths
    else:
        # Multiple files - return directory, cleanup zip only
        return extract_to, cleanup_paths


def process_file(file_path):
    """Process the file - add your custom processing logic here."""
    print(f"Processing {file_path}...")
    
    db_connection_str = ""
    with open("../Credentials/cred.txt", "r") as f:
        db_connection_str = f.readline()

    etl = HospitalChargeETLCSV(db_connection_str, file_path)
    result = etl.execute()
    
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
    result_queue = Queue()
    
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
                # Download thread can only start next download if queue has space
                process_file(file_path)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
            finally:
                # Cleanup immediately after processing
                if file_path and file_path not in cleanup_paths:
                    cleanup_paths.append(file_path)
                cleanup(cleanup_paths)
        else:
            # Error case - still cleanup what we can
            if cleanup_paths:
                cleanup(cleanup_paths)
        
    # Wait for threads to finish
    feeder_thread.join()
    download_thread.join()


def sequential_process(urls, download_dir="./downloads", target_extensions=None):
    """
    Download and process files sequentially (for comparison).
    Each file is fully downloaded and processed before starting next.
    
    Args:
        urls: List of URL strings to download
        download_dir: Directory to download files to
        target_extensions: List of file extensions to extract from zips
    """
    os.makedirs(download_dir, exist_ok=True)
    
    for url in urls:
        extracted_path = None
        cleanup_paths = []
        
        try:
            # Download
            downloaded_file = download_file(url, download_dir)
            
            # Unzip
            extracted_path, cleanup_paths = unzip_if_needed(
                downloaded_file,
                target_extensions=target_extensions
            )
            
            # Process
            process_file(extracted_path)
            
        except Exception as e:
            print(f"Error with {url}: {e}")
        
        finally:
            # Cleanup
            if extracted_path and extracted_path not in cleanup_paths:
                cleanup_paths.append(extracted_path)
            cleanup(cleanup_paths)


def main():
    # Configuration - just list URLs, filenames are auto-detected
    urls = []
    file_name = "cms-hpt.txt"
    download_dir = "./downloads"

    download_file("https://www.mountcarmelhealth.com/sites/default/files/cms-hpt.txt", download_dir)
    with open(download_dir + "/" + file_name, "r") as f:
        for line in f:
            if "location-name" in line:
                print(f"Adding MRF for {line[line.find(":") + 1:].strip()} to the list of urls")
            if "mrf-url" in line:
                urls.append(line[line.find(":") + 1:].strip())

    
    # Optional: specify which file types to extract from zips
    # This will ignore readme files, metadata, etc.
    target_extensions = ['.csv', '.json', '.txt']  # Or None to extract everything
    
    print("=" * 50)
    print("PIPELINED PROCESSING (download next while processing current)")
    print("=" * 50)
    # max_buffered=1 means download at most 1 file ahead
    # Increase if you have more disk space and want more parallelism
    pipeline_process(urls, download_dir, max_buffered=1, target_extensions=target_extensions)
    
    # Uncomment below to compare with sequential processing
    # print("\n" + "=" * 50)
    # print("SEQUENTIAL PROCESSING (download then process, one at a time)")
    # print("=" * 50)
    # sequential_process(urls, download_dir, target_extensions=target_extensions)


if __name__ == "__main__":
    main()