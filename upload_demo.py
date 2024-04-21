# For demo
import argparse
import os
from s3.s3uploader import S3Uploader

if __name__ == '__main__':
    # Parse Inputs
    ap = argparse.ArgumentParser()
    ap.add_argument("-p", "--input_path",
                    help="Root path of data product")
    args = vars(ap.parse_args())
    root_path = args["input_path"]

    if not root_path:
        root_path = os.path.dirname(os.path.abspath(__file__))
    print(root_path)
    s3 = S3Uploader()
    s3.upload_files(root_path=root_path)
    #s3.batch_upload_files(root_path=root_path, max_workers =2)
