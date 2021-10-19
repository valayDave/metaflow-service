from env_buider import DockerTestEnvironment,METAFLOW_VERSIONS,METADATA_DOCKER_TAGS,save_json
import click
import uuid
import os


@click.command()
@click.option('--database-password',default="ByvI)Sr_uamaPx$w&Xp_LoB*DVBzTO+3oK{Z_Nw4SRcxut?-B>h]&WD}_mU!AgOm'\"")
@click.option('--flow-dir',default='./test_flows')
@click.option('--with-md-logs',is_flag=True)
@click.option('--dont-remove-containers',is_flag=True)
@click.option('--build-md-image',is_flag=True)
@click.option('--md-docker-images',type=str,default=','.join(METADATA_DOCKER_TAGS))
@click.option('--temp-env-store',default='./tmp_verions')
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
            build_md_image=False,
            md_docker_images = None,
            dont_remove_containers=False,
            with_md_logs = False,\
            database_user='metaflow', \
            database_port=5432, 
            image_build_path='../', 
            docker_file_path=os.path.abspath('../Dockerfile'),\
            results_output_path=None):
    
    test_env_args = dict(
        logger=click.secho,
        database_password=database_password,\
        flow_dir = flow_dir,\
        with_md_logs=  with_md_logs,\
        versions=versions.split(','),
        dont_remove_containers=dont_remove_containers,\
        temp_env_store = temp_env_store,\
        database_name = database_name,\
        database_user = database_user,\
        database_port = database_port,\
        image_build_path = image_build_path,\
        docker_file_path = docker_file_path,\
    )
    environments = []
    final_results = []
    if build_md_image:
        environments.append(
            DockerTestEnvironment(build_md_image=build_md_image,md_docker_image=None,**test_env_args,)
        )
    else:
        for img_tag in md_docker_images.split(','):
            environments.append(
                DockerTestEnvironment(build_md_image=build_md_image,md_docker_image=img_tag,**test_env_args,)
            )
    for env in environments:
        results = env.lifecycle()
        for result in results:
            result['service_version']='HEAD'
            if env.md_docker_image is not None:
                result['service_version'] = env.md_docker_image
            final_results.append(result)

    if results_output_path is None:
        results_output_path = f"results-{str(uuid.uuid4())[:4]}.json"
    save_json(final_results,results_output_path)

if __name__ == '__main__':
    run_tests()
