import os
import pathlib
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

from boto3 import client
from loguru import logger


class CacheManager:
    """
    Manages a file-based local cache for remote s3 files.
    On cache miss, it starts a background thread to download the file from s3
    Meanwhile, it will return the remote file path to the caller so that they
    can
    continue processing the file.
    """

    CACHE_CLEANUP_INTERVAL: int = 60 * 60  # 1 hour
    MAX_AGE_OF_FILES_TO_KEEP: int = 3 * 24 * 60 * 60  # 3 days

    def __init__(self, cache_dir: pathlib.Path):
        os.makedirs(cache_dir, exist_ok=True)
        self.cache_dir = cache_dir
        self.s3_client = client("s3")
        self.cache: dict[str, str] = {}
        self.downloads_in_progress: dict[str, Any] = {}
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.lock = threading.Lock()
        self.last_cache_cleanup_time = 0

    def _delete_old_files(self, max_age_seconds: int) -> None:
        """
        Deletes files from the cache directory that are older than a certain time.

        Args:
        - max_age_seconds (int): Maximum age of files to keep in seconds.
        """
        if (
            int(time.time() * 1000) - self.last_cache_cleanup_time
            < self.CACHE_CLEANUP_INTERVAL
        ):
            # Skip cache cleanup if it was done recently
            return
        for root, _dirs, files in os.walk(self.cache_dir):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                # Get the last modification time of the file
                file_modified_time = os.path.getmtime(file_path)
                # Calculate the age of the file in seconds
                age_seconds = time.time() - file_modified_time
                if age_seconds > max_age_seconds:
                    # Delete the file if it's older than the specified threshold
                    # First remove the file from the cache dictionary
                    for s3_path, local_path in self.cache.items():
                        if local_path == file_path:
                            del self.cache[s3_path]
                            break
                    os.remove(file_path)
                    logger.info(f"Deleted old file: {file_path}")
        self.last_cache_cleanup_time = int(time.time() * 1000)

    def get_local_path(
        self, s3_path: str, fresher_than_millis: Optional[int] = None
    ) -> str:
        """
        Returns the local path of the file.
        If the file is not present in the cache, it will start a background
        thread to download the file from s3.
        If the fresher_than_millis parameter is provided, the file will be
        re-downloaded if it is older than the specified time.
        """
        if s3_path in self.cache:
            local_fs_path = self.cache[s3_path]
            if fresher_than_millis:
                logger.debug(f"Remote file freshess: {fresher_than_millis}")
                # Check if the file is older than the specified time
                local_file_time_millis = int(os.path.getmtime(local_fs_path) * 1000)
                if fresher_than_millis > local_file_time_millis:
                    logger.info(
                        f"Local file is stale: {local_fs_path}. Re-downloading."
                    )
                    # Re-download the file
                    self._enqueue_download(s3_path, local_fs_path)
                    # Return the remote path to the caller
                    return s3_path
            return local_fs_path
        else:
            # if path does not contain glob patterns, then enqueue download
            if "*" not in s3_path:
                # Start a background thread to download the file
                self._enqueue_download(
                    s3_path=s3_path, local_path=self._compute_local_path(s3_path)
                )
            return s3_path

    def _compute_local_path(self, s3_path: str) -> str:
        """
        Computes the local path of the file based on the s3 path
        for an s3 path like s3://bucket-name/path/to/file, the local path
        will be cache_dir/path/to/file
        """
        # Remove "s3://" prefix
        s3_path = s3_path.replace("s3://", "")
        # Extract bucket name and file path
        parts = s3_path.split("/")
        parts[0]
        file_path = "/".join(parts[1:])
        # Compute local path
        local_path = os.path.join(self.cache_dir, file_path)
        return local_path

    def _enqueue_download(self, s3_path: str, local_path: str) -> None:
        local_path = self._compute_local_path(s3_path)
        with self.lock:  # Acquire the lock before updating the dictionary
            if s3_path in self.downloads_in_progress:
                # download already in progress
                return
            # Submit a new download task to the executor
            future = self.executor.submit(self._download_file, s3_path, local_path)
            # Store the Future object associated with the download task
            self.downloads_in_progress[s3_path] = future

    def _download_file(self, s3_path: str, local_path: str) -> None:
        """
        Downloads a file from S3 and saves it to the local cache directory.
        """
        # Download file from S3
        bucket_name, object_key = self._parse_s3_path(s3_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        try:
            self.s3_client.download_file(bucket_name, object_key, local_path)
            print(f"Download completed: {s3_path}")
            self.cache[s3_path] = local_path
            # Opportunistically delete old files from the cache
            self._delete_old_files(self.MAX_AGE_OF_FILES_TO_KEEP)
        except Exception as e:
            print(f"Error downloading {s3_path}: {e}")
        finally:
            with self.lock:
                # Remove the completed download task from downloads_in_progress
                del self.downloads_in_progress[s3_path]

    def _parse_s3_path(self, s3_path: str) -> tuple:
        """
        Parses S3 path into bucket name and object key.
        """
        # Remove "s3://" prefix
        s3_path = s3_path.replace("s3://", "")
        # Extract bucket name and object key
        parts = s3_path.split("/", 1)
        bucket_name = parts[0]
        object_key = parts[1]
        return bucket_name, object_key

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def close(self) -> bool:
        self.executor.shutdown(wait=True)
        # delete the cache directory
        shutil.rmtree(self.cache_dir)
        return False
