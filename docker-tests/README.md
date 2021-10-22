# Metaflow Integration Test Harness

## Why this test harness ? 

Metaflow's architecture comprises of various components like the Metaflow client, the metadata service and the database hosting the metaflow `Run`/`Task`/`Step` etc. The components of metaflow are individually version controled making it harder to keep a track of what versions of individualy components are compatible with other components. This integration test harness helps tackle this problem. Via this harness it provides a better visibility to what versions of different components are inter-compatible with other versions.

## What does this do ? 

There are three scripts which perform the following tasks: 

1. `run_test.py` **[INTEGRATION TEST]**: Runs an integration test where it _either_ tests all metaflow client versions with metadata service versions _or_ tests all the metaflow client versions with current MD service version of the current git HEAD in the working directory. 

2. `fixed-db-tests.py` **[INTEGRATION TEST]**: This test sets up one database and then sequentially tests all metaflow versions with all versions of the Metadata Service's dockerhub images. It can also build the md service by itself and then test all versions of the metaflow client. As the tests are run sequentially over the same database, it also allows us to run migrations as a part of the tests. These help ensure replication of behaviour based on the break changes introduced by v2.0.0. 

3. `deploy-ui.py`: This script will deploy the metaflow UI. It expects an already available postgres database with metaflow on it. The UI on deployment will be available on `http://localhost:8083`. 

## How to run the tests ? 

### Quick Run 

1. Install requirements via `pip install -r requirements.txt`

2. Running the tests Individually 
    1. Run the test with local datastore. :

    ```sh
    # This will run the integration tests with local datastore for the Metaflow client and finally wont delete the containers. 
    python fixed-db-tests.py --database-password password \
                            --datastore local \
                            --dont-remove-containers
    ```

    2. Run the test with `s3` as datastore. (Please note you need to have AWS credentials in env variables to run the test with S3 datastore)
    ```sh
    # This will run the integration tests with s3 datastore for the Metaflow client and finally wont delete the containers. 
    python fixed-db-tests.py --database-password password \
                            --datastore s3 \
    ```

3. Running full integration tests and checking the output of the tests in the end with Metaflow-UI:

```sh
sh run_tests.sh
``` 



#### Important Related Information 

[sequential_test.py](./sequential_test.py) contains information about Metadata-service-versions and if they require a migration to run that versions. To run a dry run of the tests just comment out all items except first or last one and then run the following command 
```sh
  python fixed-db-tests.py --database-password password \
                            --datastore local \
                            --versions 2.2.1,2.2.2 \
                            
```
