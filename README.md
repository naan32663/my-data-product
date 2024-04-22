
How to run the project?

1. Create your virtualenv and activate your virtualenv, install all libs from requirements.txt by below command.

pip install -r requirements.txt

2. Switch to the project directory and modify the S3 credentials in the conf.json

3. Run python upload_demo.py -p your/path/my-data-product

4. To run unit tests, run pytest in the project root directory.

Improvements

1. Develop a config class to load all configuration information. This class could use the singleton pattern to ensure that all services get the same configuration information. Load configuration when the app starts to avoid wasting resources with multiple loads.

2. I use hard code to define the source folder in the program, it can be configured using the configuration file.
