import abc
import docker
from docker.errors import BuildError, NotFound, APIError
import time
from versioned_tests import EnvConfig,MFTestRunner
POSTGRES_IMAGE = 'postgres:9-alpine'
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
                max_ip_wait_time = 20,\
                database_name='metaflow',
                database_password='password',
                database_user='metaflow',
                database_port = 5432,
                image_name = 'metaflow_metadata_service',
                network_name='postgres-network',\
                docker_file_path=os.path.abspath('../Dockerfile')) -> None:
        
        self._docker = docker.DockerClient(base_url='unix://var/run/docker.sock')
        
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
        self._metadataservice_name = 'testharness-metadataservice'
    
    def lifecycle(self):
        # Create the network and the image. 
        self._create_enivornment()
        self._run_tests()
        self._teardown_environment()
        
    def _run_tests(self):
        pass

    def _teardown_environment(self):
        container_set = [
            self._metadataservice_container,
            self._database_container
        ]
        # first stop all containers
        for container in container_set:
            container.stop(timeout=10)
            container.reload()
        # Then remove all the containers
        for container in container_set:
            container.remove()
        
        # Remove the network 
        self._network.remove()

        
        # remove the images.
        self._docker.images.remove(self._metadata_image.name)
        
    
    def _db_env_vars(self):
        return dict(
            POSTGRES_USER=self._database_password,
            POSTGRES_PASSWORD=self._database_user,
        )
    
    def _resolve_db_ipaddr(self):
        # Wait for 20 seconds until the IP addr of the 
        # database container is available
        for i in range(self._max_ip_wait_time):
            ipaddr = self._database_container.attrs['Networks']['IPAddress']
            if ipaddr == '':
                self._database_container.reload()
            else:
                return ipaddr
            time.sleep(1)
        raise IpNotResolved(container_name=self._database_container.name)

    def _mdcontainer_env_vars(self):
        ip_addr = None
        for container in self._network.containers:
            if container.id == self._database_container.id:
                ip_addr = self._resolve_db_ipaddr()

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

    def _create_enivornment(self):
        self._network = self._find_network()
        
        if self._network is None:
            self._network = self._create_network()
        
        # Build the image of the Metadata service.
        self._metadata_image = self._build_mdservice_image()

        # Create the Postgres container
        self._database_container = self._docker.containers.run(POSTGRES_IMAGE,\
                                            detach=True,\
                                            name=self._database_container_name,\
                                            environment=self._db_env_vars(),\
                                            network=self._network_name,)
        
        # Create the metadata service container
        self._metadataservice_container = self._docker.containers.run(self._image_name,\
                                                    detach=True,\
                                                    envionment=self._mdcontainer_env_vars(),\
                                                    ports=self._mdservice_ports(),\
                                                    )
        
    
    def _build_mdservice_image(self):
        dockerfileobj = open(self._docker_file_path)
        image,log_generator = self._docker.images.build(path=self._docker_file_path,fileobj=dockerfileobj,tag=self._image_name,)
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
    test_runner = DockerTestEnvironment()
    test_runner.lifecycle()

if __name__ == '__main__':
    run_tests()