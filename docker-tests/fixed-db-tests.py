"""
This test sets up one database and then sequentially tests all metaflow versions with all versions of the Metadata Service's dockerhub images. It can also build the md service by itself and then test all versions of the metaflow client. 

As the tests are run sequentially over the same database, it also allows us to run migrations as a part of the tests. These help ensure replication of behaviour based on the break changes introduced by v2.0.0. 

These test make it easier to find out which db schema versions are compatible based on which initial service version the user started from. 
"""
from sequential_test import METAFLOW_VERSIONS, execute_sequential_test
from env_buider import save_json
import click
from functools import partial
import uuid
import datetime
import os
import logging
TIME_FORMAT = "%Y-%m-%d %I:%M:%S %p"

def logger(base_logger,*args,**kwargs):
    msg = f'{datetime.datetime.now().strftime(TIME_FORMAT)} - Metaflow Integration Test Harness - {args[0]}'
    base_logger(msg,**kwargs)


@click.command()
@click.option('--database-password',default="ByvI)Sr_uamaPx$w&Xp_LoB*DVBzTO+3oK{Z_Nw4SRcxut?-B>h]&WD}_mU!AgOm'\"")
@click.option('--flow-dir',default='./test_flows')
@click.option('--with-md-logs',is_flag=True)
@click.option('--temp-env-store',default='./tmp_versions')
@click.option('--database-name',default='metaflow')
@click.option('--database-user',default='metaflow')
@click.option('--database-port',default=5432)
@click.option('--versions',default=','.join(METAFLOW_VERSIONS))
@click.option('--image-build-path',default='../')
@click.option('--docker-file-path',default=os.path.abspath('../Dockerfile'))
@click.option('--results-output-path',default=None)
def run_tests(database_password=None, \
            flow_dir='./test_flows', \
            temp_env_store='./tmp_verions', \
            database_name='metaflow', \
            versions=None,\
            with_md_logs = False,\
            database_user='metaflow', \
            database_port=5432, 
            image_build_path='../', 
            docker_file_path=os.path.abspath('../Dockerfile'),\
            results_output_path=None):
    echo = partial(logger,click.secho)
    echo("Starting The Sequential DB + MD Compatibility Test",fg='green')
    test_env_args = dict(
        logger=echo,
        database_password=database_password,\
        flow_dir = flow_dir,\
        with_md_logs=  with_md_logs,\
        versions=versions.split(','),
        dont_remove_containers=False,\
        temp_env_store = temp_env_store,\
        database_name = database_name,\
        database_user = database_user,\
        database_port = database_port,\
        image_build_path = image_build_path,\
        docker_file_path = docker_file_path,\
    )
    final_results = execute_sequential_test(**test_env_args)
    if results_output_path is None:
        results_output_path = f"results-{str(uuid.uuid4())[:4]}.json"
    save_json(final_results,results_output_path)

     

if __name__ == '__main__':
    run_tests()
