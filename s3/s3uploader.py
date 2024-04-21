import boto3
import concurrent.futures
import logging
import os

from concurrent.futures import ThreadPoolExecutor
from typing import Tuple, List

from s3.exceptions import S3Exception
from s3.utils import get_s3_config

SOURCE_FOLDER = ["airflow/src","dbt"]
EXCLUDE_FOLDER = "logs"

class S3Uploader:
    def __init__(self):
        self._s3_conf = None
        self._s3_client  = None
        self._bucket_name = None


    @property
    def s3_conf(self):
        """S3 configuration"""
        if not self._s3_conf:
            self._s3_conf = get_s3_config()

        return self._s3_conf
    

    @property
    def s3_client(self):
        """S3 client object"""
        if not self._s3_client:
            key = self.s3_conf.get("aws_access_key_id")
            secret = self.s3_conf.get("aws_secret_access_key")
            region = self.s3_conf.get("region")
            
            for item in [key, secret, region]:
                if not item:
                    raise S3Exception("S3 credentials not found.")
            
            self._s3_client = boto3.client("s3", 
                                           aws_access_key_id=key, 
                                           aws_secret_access_key=secret, 
                                           region_name=region)
        
        return self._s3_client
    

    @property
    def bucket_name(self):
        """S3 bucket name"""
        if not self._bucket_name:
            self._bucket_name = self.s3_conf.get("bucket")

            if not self._bucket_name:
                raise S3Exception("S3 bucket name not found.")
        
        return self._bucket_name


    def upload_files(self, root_path: str) -> None:
        """Upload files to AWS S3

        Args:
            root_path: Root path of data product
        """
        files_list, s3_key_list = self._get_files(root_path)
        total_amount = len(files_list)
        failed_amount = 0

        for local_file, s3_key in zip(files_list, s3_key_list):
            try:
                self._copy_to_bucket(local_file, s3_key)
                logging.debug(f"Uploaded {local_file} as {s3_key}")
            except Exception as e:
                logging.error(f"Error uploading {local_file}: {e}")
                failed_amount +=1
                
        logging.info(f"{total_amount} files find, {failed_amount} files failed to upload.")


    def batch_upload_files(self, root_path: str, max_workers: int=5) -> None:
        """Upload files to AWS S3 by batch

        Args:
            root_path: Root path of data product
            max_workers: Maximum number of threads  
        """
        files_list, s3_key_list = self._get_files(root_path)
        total_amount = len(files_list)
        failed_amount = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for local_file, s3_key in zip(files_list, s3_key_list):
                futures.append(executor.submit(self._copy_to_bucket, 
                                               local_file, 
                                               s3_key))

            for future in concurrent.futures.as_completed(futures):
                try: 
                    future.result()
                except Exception as e:
                    logging.error(f"Task failed: {e}")
                    failed_amount += 1
            logging.info(f"{total_amount} files find, {failed_amount} files failed to upload.")


    def _copy_to_bucket(self, local_file: str, s3_key: str) ->None:
        """Copy single file to AWS s3
        
        Args:
            bucket: S3 bucket
            local_file: local file path
            s3_key: S3 path
        """
        self.s3_client.upload_file(
            local_file,
            self.bucket_name,
            s3_key,
        )


    def _get_files(self, root_path: str) -> Tuple[List[str], List[str]]:
        """Get the source files to upload and the s3 key for source files
        
        Args:
            root_path: Root path of data product
            source_folders: Source folder relative path list
            exclue_folder: Folder to exclude
        
        Return:
            Tuple[List[str], List[str]]: list of source file path and target file path

        Raise:
            FileNotFoundError: If the root path cannot be found
        """
        files_to_upload = []
        s3_keys =[]
        if not os.path.exists(root_path):
            raise FileNotFoundError

        for path in SOURCE_FOLDER:
            for root, dirs, files in os.walk(os.path.join(root_path, path)):
                if EXCLUDE_FOLDER in dirs:
                    dirs.remove(EXCLUDE_FOLDER)
            
                for file in files:
                    file_path = os.path.join(root, file)
                    files_to_upload.append(file_path)
                    s3_key = os.path.relpath(file_path, root_path).replace("\\","/")
                    s3_keys.append(s3_key)

        return files_to_upload, s3_keys