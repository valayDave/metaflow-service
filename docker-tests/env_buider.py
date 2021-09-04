import abc
import shutil
import docker
import click
from docker.errors import BuildError, NotFound, APIError
import time
import traceback

from docker.models.containers import Container
from versioned_tests import EnvConfig,MFTestRunner,METAFLOW_VERSIONS
POSTGRES_IMAGE = 'postgres:latest'
import os 
class IpNotResolved(Exception):
    headline = 'IP Address of Container Unresolvable'
    def __init__(self, container_name='',container_id='', lineno=None):
        self.message = f"Cannot Resolve IP Address of container : {container_name} : {container_id}"
        self.line_no = lineno
        super(IpNotResolved, self).__init__()

class DockerTestEnvironment:
    """ 
    Lifecycle :
        create_env 
            --> setup network
            --> setup image of the repo
            --> create database container
            --> create mdservice container from image

        run_tests
            --> containers setup tests
            --> 
            --> 
            -->
        
        teardown_env
            --> stop all containers 
            --> remove all containers
            --> remove the network
            --> remove the image
    """

    def __init__(self,\
                versions = METAFLOW_VERSIONS,\
                flow_dir = './test_flows',\
                temp_env_store='./tmp_verions',\
                max_ip_wait_time = 20,\
                database_name='metaflow',
                database_password='password',
                database_user='metaflow',
                database_port = 5432,
                logger=None,
                image_name = 'metaflow_metadata_service',
                network_name='postgres-network',\
                image_build_path='../',\
                docker_file_path=os.path.abspath('../Dockerfile')) -> None:
        
        self._docker = docker.DockerClient(base_url='unix://var/run/docker.sock')

        self._logger = logger if logger is not None else lambda *args:print(*args)
        
        # Network Related Properties
        self._network = None
        self._network_name = network_name

        # database related configurations.
        self._database_container = None
        self._database_container_name = 'testharness-postgres'
        self._database_name = database_name
        self._database_password = database_password
        self._database_user = database_user
        self._database_port = database_port
        self._max_ip_wait_time = max_ip_wait_time

        # Configuration related to the containers for the test harness. 
        self._docker_file_path = docker_file_path
        self._image_name = image_name # Image of MD Service
        self._metadataservice_container = None
        self._metadata_image = None
        self._image_build_path = image_build_path
        self._metadataservice_name = 'testharness-metadataservice'

        # Local Test Harness Related Configuration
        self._flow_dir = flow_dir
        self._mf_versions = versions
        self._temp_env_store = temp_env_store
    
    def lifecycle(self):
        # Create the network and the image. 
        self._logger('Creating New Environment',fg='green')
        self._create_enivornment()
        try:
            self._logger('Environment Created, Now Running Tests',fg='green')
            self._run_tests()
            self._logger('Finished Running Test ! Wohoo!',fg='green')
        except Exception as e:
            error_string = traceback.format_exc()
            self._logger(f'Something Failed ! {error_string}',fg='red')
        finally:
            self._logger("Tearing down environment",fg='green')
            self._teardown_environment()
        
        
    def _run_tests(self):
        url = f"http://localhost:8080/"
        test_runner = MFTestRunner(
            self._flow_dir,
            EnvConfig(
                datastore='local',
                metadata='service',
                metadata_url=url
            ),
            versions=self._mf_versions,
            temp_env_store=self._temp_env_store,
        )
        test_runner.run_tests()

    def _teardown_environment(self):
        container_set = [
            self._metadataservice_container,
            self._database_container
        ]
        self._logger('Stopping all containers',fg='blue')
        # first stop all containers
        for container in container_set:
            container.stop(timeout=10)
            container.reload()
        
        self._logger('Removing all containers',fg='blue')
        # Then remove all the containers
        for container in container_set:
            container.remove()
        
        self._logger('Removing Network',fg='blue')
        # Remove the network 
        self._network.remove()

        self._logger('Removing Docker Images',fg='blue')
        # remove the images.
        self._docker.images.remove(self._metadata_image.id)
        
        # remove temporary directory of MF versions
        is_present = False
        try: 
            os.stat(self._temp_env_store)
            is_present = True
        except FileNotFoundError as e:
            pass
        if is_present:
            shutil.rmtree(self._temp_env_store)
        
    
    def _db_env_vars(self):
        return dict(
            POSTGRES_USER=self._database_user,
            POSTGRES_PASSWORD=self._database_password ,
            POSTGRES_DB=self._database_name
        )
    
    def _resolve_ipaddr(self,container:Container,wait_time=None):
        # Wait for 20 seconds until the IP addr of the 
        # database container is available
        wait_time = wait_time if wait_time is not None else self._max_ip_wait_time
        for i in range(wait_time):
            try:
                ipaddr = container.attrs['NetworkSettings']['Networks'][self._network_name]['IPAddress']
            except KeyError:
                ipaddr = ''

            if ipaddr == '':
                self._logger(f"Couldn't resolve IP Address for container {container.name} of image {container.image.tags}. Waiting for {wait_time-i} seconds",fg='red')
                container.reload()
            else:
                return ipaddr
            time.sleep(1)
        raise IpNotResolved(container_name=container.name,container_id=container.id)

    def _mdcontainer_env_vars(self):
        ip_addr = None
        ip_addr = self._resolve_ipaddr(self._database_container,wait_time=120)
        self._logger(f'Using DB Ip Address {ip_addr} ',fg='green')
        return dict(
            MF_METADATA_DB_HOST = ip_addr,
            MF_METADATA_DB_PORT = self._database_port,
            MF_METADATA_DB_USER = self._database_user,
            MF_METADATA_DB_PSWD = self._database_password,
            MF_METADATA_DB_NAME = self._database_name,
        )

    def _mdservice_ports(self):
        return {
            '8082/tcp':8082,
            '8080/tcp':8080,
        }
    def _db_ports(self):
        return {
            f'{self._database_port}/tcp':self._database_port,
        }

    def _create_enivornment(self):
        self._logger('Creating a network',fg='blue')
        self._network = self._find_network()
        
        if self._network is None:
            self._network = self._create_network()
        
        # Build the image of the Metadata service.
        self._logger('Building Metadata Image',fg='blue')
        self._metadata_image = self._build_mdservice_image()

        self._logger('Creating Postgres Docker Container',fg='blue')
        # Create the Postgres container
        self._database_container = self._docker.containers.run(POSTGRES_IMAGE,\
                                            detach=True,\
                                            ports=self._db_ports(),\
                                            name=self._database_container_name,\
                                            environment=self._db_env_vars(),\
                                            network=self._network_name,)
        
        time.sleep(10)
        self._logger('Creating Metadata Service Container',fg='blue')
        # Create the metadata service container
        self._metadataservice_container = self._docker.containers.run(self._image_name,\
                                                    detach=True,\
                                                    stdin_open=True,\
                                                    tty=True,\
                                                    environment=self._mdcontainer_env_vars(),\
                                                    network=self._network_name,\
                                                    ports=self._mdservice_ports(),\
                                                    )
        
        time.sleep(40)
    
    def _build_mdservice_image(self):
        image,log_generator = self._docker.images.build(path=self._image_build_path,\
                                                        dockerfile=self._docker_file_path,\
                                                        tag=self._image_name,)
        return image
    
    def _find_network(self):
        try:
            return self._docker.networks.get(self._network_name)
        except NotFound as e:
            return None
        except:
            raise

    def _create_network(self):
        return self._docker.networks.create(self._network_name)

def run_tests():
    test_runner = DockerTestEnvironment(
        logger=click.secho
    )
    test_runner.lifecycle()

if __name__ == '__main__':
    run_tests()
