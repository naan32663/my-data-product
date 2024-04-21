import json
import os

from typing import Any, Dict, Optional

CONF_FILE = "../conf.json"
PROJECT_NAME = "my-data-product"

def get_s3_config(config_file:Optional[str] = CONF_FILE) -> Dict[str, str]:
    """Return S3 config
    
    Args:
        config_file: path of config file
    Return:
        Dict[str,str]: S3 credentials and bucket name
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        with open(os.path.join(current_dir,config_file), "r") as f:
            conf = json.load(f)
            return conf.get("s3")

    except Exception as e:
        raise e

def get_root_path() -> Any:
    """Return the project path
    
    Return:
        Any: project path if exists otherwise none    
    """    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    while True:
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            return None
        if os.path.exists(os.path.join(parent_dir, PROJECT_NAME)):
            return os.path.join(parent_dir,PROJECT_NAME)
        current_dir = parent_dir
