

# Data demo environment
The purpose of this repo is to quickly set up a demo environment to be able to play around with various AWS data services like S3, Athena, Glue etc. 

This is done by deploying a storage layer (S3 buckets), a dataset, some sample Glue crawlers and Athena configurations. This will be extended in the future. Currently the following is supported:

*Storage and databases*: 
- S3 'raw' data bucket is populated with raw IMDB dataset
- S3 'processed' data bucket (initially empty)
- Optional RDS database to store data in, with connectivity to the glue job using the `.with_aurora()` method. 
- Optional dynamodb tabe to store data in, using the `.with_dynamodb()` method.

*AWS Glue:*
- Classification of raw dataset with glue crawlers (optionally with custom classifier using the `.withCustomClassifier()` method).
- Glue ETL job to convert the `name` dataset from `tsv` to `parquet` format (see the `scripts` folder) using the `.withGlueJobForNameBasics()` method
    - VPC support to connect to private databases using the `.withVpc()` method. 
- IAM Role is created to be used with Glue services, with access to the raw and processed (empty) data bucket. The role can be used with:
    - Glue studio (e.g. preview data)
    - Glue databrew
    - Optionally provide admin access to the role if yoou want to experiment with services outside of this repo using the `.withAdminPrivileges()` method.
- Empty 'scripts and query' bucket to store Glue scripts in.
- Installing custom python packages using wheel files
- Glue V1 development endpoint (legacy) using the `.withGlueV1DevEndpoint()` method. 

*Athena*: 
- Athena workgroup and sample queries are deploy to query and join IMDB datasets using the `.withAthena()` method
    - Support for federated query to dynamodb using the `.withAthena(c)` method

To explore the supported methods and parameters in more detail, check the `stacks/demo_stack.py` file.
# Deployment

*Prerequisites*
Run the following script.
```
git clone https://github.com/LRuttenCN/cdk-serverless-data-lake-sandbox.git
cd cdk-serverless-data-lake-sandbox
python3 -m pip install -r requirements.txt
sudo npm install -g aws-cdk@2.25.0
```
This script:
- Clones the repository  
- Install all requirements 
- Make sure you have a compatible version of the cdk cli installed (`cdk --version`)

> NOTE: it is assumed that you already have the `aws cli`, `python3`, `pip` and `npm` on your local machine.

## Setup data (assets)
To be able to use sample data, use either of the two methods below to populate the serverless data lake with sample data:

**OPTION 1:**
First, create  the `assets` folder by running:
`mkdir -p assets && cd assets`


Select the datasets you want to investigate from this [webpage](https://datasets.imdbws.com/).
For each dataset, run:
```
mkdir <dataset_name> && cd <dataset_name>
wget https://datasets.imdbws.com/<dataset name>
gzip -d <dataset name>
```

Available datasets are:
```
(*) name.basics.tsv.gz
title.akas.tsv.gz
(*)title.basics.tsv.gz
title.crew.tsv.gz
title.episode.tsv.gz
title.principals.tsv.gz
(*) title.ratings.tsv.gz
```
For this repository, only the ones with marked with an asterisk (*) are required.

**OPTION2:**

Run the `./get_imdb_datasets` script

## Deploy the app
1. Modify `app.py`:
    - `env_EU` to the region of choice
    - `DEMO_ID` a unique and recognizable identifier for your use-case (e.g. `sandbox`)
    - Customize your demo environment by adding or removing components to the stack (e.g. `DemoStack(...).withAthena().withCustomClassifier().withGlueJobForNameBasics()`)
2. Run `npx cdk diff` and then `npx cdk deploy`. This takes about 20 minutes, depending on your upload speed (±2GB of data)

# On Custom Classifiers
By default, raw data is classified with glue crawler without custom classifier. For some datasets, all columns are strings. This causes the classifier to not detect the header row (which is also all strings). Therefore, a crawler with custom classifier can be deployed which enforces header detection.

The difference in classification can for example be seen in the `name.basics` dataset.

# Generating wheel files for python shell
To install additional dependencies in your glue environment, you can use wheel files. Below you can find a brief guide on how to create these:

First, go to the `packages` directory by running
```
cd glue_scripts/packages
```
Modify the `setup.py` file (or create your own alternative file) with the packages and versions you want installed on your glue job.

Then run a docker container with the python runtime version of choice. In the example below, I chose to use pythoon 3.6. It mounts to your local directory and builds the wheel file. 
You can also build the file without docker, but make sure you have the same version of python running locally as you use in AWS Glue. 
```
docker run \
    --mount type=bind,source="$(pwd)",target=/app \
    python:3.6 /bin/bash -c "cd /app && python setup.py bdist_wheel"
```

Then copy over the dist files using:
```
aws s3 cp dist/ s3://<GLUE ASSETS BUCKET NAME>/modules/ --resursive
```