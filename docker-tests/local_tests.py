from versioned_tests import TIME_FORMAT, MFTestRunner, EnvConfig, METAFLOW_VERSIONS
import datetime
from functools import partial
import click
def logger(base_logger,*args,**kwargs):
    msg = f'{datetime.datetime.now().strftime(TIME_FORMAT)} - Metaflow Integration Test Harness - {args[0]}'
    base_logger(msg,**kwargs)

# todo : create options to use a metaflow profile to choose configuration. 
@click.command()
@click.option('--datastore',type=click.Choice(['local','s3']),default='local')
@click.option('--metadata',type=click.Choice(['local','service']),default='local')
@click.option('--execute_on',type=click.Choice(['local','step-functions']),default='local')
@click.option('--versions',default=','.join(METAFLOW_VERSIONS))
def run_tests(datastore=None,metadata=None,execute_on=None,versions=None):
    from metaflow.metaflow_config import DATASTORE_SYSROOT_S3,DATATOOLS_S3ROOT,METADATA_SERVICE_URL,METADATA_SERVICE_AUTH_KEY
    echo = partial(logger,click.secho)
    echo(f'Testing metaflow with Datastore {datastore} and Metadata {metadata} , Executed on : {execute_on}',fg='green')
    test_runner = MFTestRunner(
        './test_flows',
        EnvConfig(
            execute_on=execute_on,
            datastore=datastore,
            metadata=metadata,
            s3_datastore_root=DATASTORE_SYSROOT_S3,
            s3_datatools_root=DATATOOLS_S3ROOT,
            metadata_url=METADATA_SERVICE_URL,
            metadata_auth=METADATA_SERVICE_AUTH_KEY
        ),
        logger=echo,
        versions=versions.split(','),
        )
    test_results = test_runner.run_tests()
    for res in test_results:
        message = f"Successfully executed flow {res['flow']}/{res['run']} with Metaflow version {res['version']}"
        fg='green'
        if not res['success']:
            message = f"Failed in executing flow {res['flow']}/{res['run']} with Metaflow version {res['version']}"
            fg='red'
        echo(message,fg=fg)
        


if __name__ == '__main__':
    run_tests()