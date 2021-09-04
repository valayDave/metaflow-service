from env_buider import DockerTestEnvironment
import click
import os

@click.command()
@click.option('--database-password',default='metaflow')
@click.option('--flow-dir',default='./test_flows')
@click.option('--temp-env-store',default='./tmp_verions')
@click.option('--database-name',default='metaflow')
@click.option('--database-user',default='metaflow')
@click.option('--database-port',default=5432)
@click.option('--image-build-path',default='../')
@click.option('--docker-file-path',default=os.path.abspath('../Dockerfile'))
def run_tests(database_password=None, \
            flow_dir='./test_flows', \
            temp_env_store='./tmp_verions', \
            database_name='metaflow', \
            database_user='metaflow', \
            database_port=5432, 
            image_build_path='../', 
            docker_file_path=os.path.abspath('../Dockerfile')):
    test_runner = DockerTestEnvironment(
        logger=click.secho,
        database_password=database_password,\
        flow_dir = flow_dir,\
        temp_env_store = temp_env_store,\
        database_name = database_name,\
        database_user = database_user,\
        database_port = database_port,\
        image_build_path = image_build_path,\
        docker_file_path = docker_file_path,\
    )
    test_runner.lifecycle()

if __name__ == '__main__':
    run_tests()
