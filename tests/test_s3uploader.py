import pytest
import os

from pytest_mock import plugin

from s3.exceptions import S3Exception
from s3.s3uploader import S3Uploader

MOCK_CONF = {"aws_access_key_id": "mockid",
            "aws_secret_access_key": "mockkey",
            "region": "mockregion",
            "bucket": "mockbucket"}
ROOT_PATH=os.path.abspath(os.getcwd())

def test_copy_to_bucket(mocker: plugin.MockerFixture) -> None:
    """
    Verify copying a single file to s3 is correct
    """
    mocks3=mocker.patch('boto3.s3.transfer.S3Transfer.upload_file')

    s3_key = "my-data-product/dbt/dbt_project.yml"
    local_file_path = "d:/my-data-product/dbt/dbt_project.yml"

    #Act
    s3Uploader = S3Uploader()
    s3Uploader._copy_to_bucket(local_file_path,s3_key)

    #Assert
    mocks3.assert_called_once()

def test_copy_to_bucket_with_error(mocker: plugin.MockerFixture) -> None:
    """
    Verify copying a single file to s3 is correct when s3 returns error
    """
    mocker.patch('boto3.s3.transfer.S3Transfer.upload_file',side_effect=Exception('Error Uploading.'))

    s3_key = "mock/s3/path/mockfile.py"
    local_file_path="mock/source/path/mockfil.py"

    #Act
    s3Uploader = S3Uploader()
    with pytest.raises(Exception, match="Error Uploading."):
        s3Uploader._copy_to_bucket(local_file_path,s3_key)


def test_init(mocker: plugin.MockerFixture) -> None:
    """
    Verify if s3Uploader initialized correctly.
    """
    mocker.patch("s3.s3uploader.get_s3_config",return_value=MOCK_CONF)

    #Act
    s3Uploader = S3Uploader()

    #Assert
    assert s3Uploader.s3_conf == MOCK_CONF
    assert s3Uploader.bucket_name =="mockbucket"

@pytest.mark.parametrize("conf", [
    {"aws_access_key_id": "mockid", "region": "mockregion"},            # aws_secret_access_key is missing 
    {"aws_secret_access_key": "mockkey","region": "mockregion"},        # aws_access_key_id is missing 
    {"aws_access_key_id": "mockid","aws_secret_access_key": "mockkey"}, # region is missing 
])
def test_init_missing_s3_credential(mocker: plugin.MockerFixture, conf) -> None:
    """
    Verify if S3Exception raised when s3 credential is missing
    """
    mocker.patch("s3.s3uploader.get_s3_config",return_value=conf)

    # Act
    with pytest.raises(
        S3Exception, match="S3 credentials not found."
    ):
        s3Uploader = S3Uploader()
        s3Uploader.s3_client

def test_upload_files(mocker: plugin.MockerFixture) -> None:
    """
    Verify if files are uploaded correctly
    """
    mock_files_list = ["/path1/source_file_1","/path2/source_file_2"] 
    mock_s3_key_list = ["/s3path1/source_file_1","/s3path2/source_file_2"]
    mocker.patch("s3.s3uploader.S3Uploader._get_files",return_value=(mock_files_list,mock_s3_key_list))
    mock_copy_to_bucket = mocker.patch("s3.s3uploader.S3Uploader._copy_to_bucket")

    # Act
    s3Uploader = S3Uploader()
    s3Uploader.upload_files(ROOT_PATH)
    
    #Assert
    assert mock_copy_to_bucket.call_count == 2

def test_upload_files_with_s3_error(mocker: plugin.MockerFixture) -> None:
    """
    Verify upload_files is correct when copy_to_bucket raise an error
    """
    mock_files_list = ["/path1/source_file_1","/path2/source_file_2"] 
    mock_s3_key_list = ["/s3path1/source_file_1","/s3path2/source_file_2"]
    mocker.patch("s3.s3uploader.S3Uploader._get_files",return_value=(mock_files_list,mock_s3_key_list))
    mock_copy_to_bucket = mocker.patch("s3.s3uploader.S3Uploader._copy_to_bucket")
    mock_copy_to_bucket.side_effect = [Exception("Mock object raised an exception"),None]

    # Act
    s3Uploader = S3Uploader()
    s3Uploader.upload_files(ROOT_PATH)

    #Assert
    mock_copy_to_bucket.assert_called_with("/path2/source_file_2", 
                                           "/s3path2/source_file_2")

def test_batch_upload_files(mocker: plugin.MockerFixture) -> None:
    """
    Verify if batch uploading files works correctly
    """
    mock_files_list = ["/path1/source_file_1","/path2/source_file_2"] 
    mock_s3_key_list = ["/s3path1/source_file_1","/s3path2/source_file_2"]
    mocker.patch("s3.s3uploader.S3Uploader._get_files",return_value=(mock_files_list,mock_s3_key_list))
    mock_copy_to_bucket = mocker.patch("s3.s3uploader.S3Uploader._copy_to_bucket")

    # Act
    s3Uploader = S3Uploader()
    s3Uploader.batch_upload_files(root_path = ROOT_PATH, max_workers = 2)
    
    #Assert
    assert mock_copy_to_bucket.call_count == 2 

def test_batch_upload_files_with_s3_error(mocker: plugin.MockerFixture) -> None:
    """
    Verify if batch uploading files coorrect when copy_to_bucket raise an error
    """
    mock_files_list = ["/path1/source_file_1","/path2/source_file_2"] 
    mock_s3_key_list = ["/s3path1/source_file_1","/s3path2/source_file_2"]
    mocker.patch("s3.s3uploader.S3Uploader._get_files",return_value=(mock_files_list,mock_s3_key_list))
    mock_copy_to_bucket = mocker.patch("s3.s3uploader.S3Uploader._copy_to_bucket")
    mock_copy_to_bucket.side_effect = [Exception("Mock object raised an exception"),None]

    # Act
    s3Uploader = S3Uploader()
    s3Uploader.batch_upload_files(root_path = ROOT_PATH, max_workers = 2)
    
    #Assert
    mock_copy_to_bucket.assert_called_with("/path2/source_file_2", 
                                           "/s3path2/source_file_2")

def test_get_files() -> None:
    """
    Verify get source files list and targets file list are correct
    """
    source_file = [
                    "airflow/src/dags/my_dag.py",
                    "airflow/src/tasks/my_task.py",
                    "dbt/models/my_model.sql",
                    "dbt/dbt_project.yml",
                    "dbt/packages.yml"
                    ]
    expect_source_path_list = [os.path.join(ROOT_PATH, path).replace("\\","/") for path in source_file]

    # Act
    s3Uploader = S3Uploader()
    source_files_list, target_files_list = s3Uploader._get_files(ROOT_PATH)
    
    #Assert
    source_files_list= [path.replace("\\","/") for path in source_files_list]

    assert sorted(source_files_list)==sorted(expect_source_path_list)
    assert sorted(target_files_list)==sorted(source_file)

def test_get_files_root_path_not_exist() -> None:
    """
    Verify get_files is correct when root path dosen't exist
    """
    root_path = "/mock/path"
    # Act
    s3Uploader = S3Uploader()
    with pytest.raises(FileNotFoundError):
        s3Uploader._get_files(root_path)
