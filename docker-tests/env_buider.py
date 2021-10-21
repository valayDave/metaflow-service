import abc
from re import L
import shutil
import docker
from docker.errors import APIError
import click
from docker.errors import BuildError, NotFound, APIError
import time
import traceback

from docker.models.containers import Container
from versioned_tests import EnvConfig,MFTestRunner,METAFLOW_VERSIONS,save_json
import os 

POSTGRES_IMAGE = 'postgres:latest'
MD_SERVICE_IMAGE_NAME = 'netflixoss/metaflow_metadata_service'
METADATA_DOCKER_TAGS = [
    "1.0.0",
    "1.0.1",
    "2.0.0",
    "2.0.1",
    "2.0.2",
    "2.0.3",
    "2.0.4",
    "2.0.5",
    "latest",
]
class IpNotResolved(Exception):
    headline = 'IP Address of Container Unresolvable'
    def __init__(self, container_name='',container_id='', lineno=None):
        self.message = f"Cannot Resolve IP Address of container : {container_name} : {container_id}"
        self.line_no = lineno
        super(IpNotResolved, self).__init__()

class IntegrationTestEnvironment:

    def __init__(self) -> None:
        self._logger = lambda x,**kwargs : print(x,**kwargs)

    def create_environment(self):
        raise NotImplementedError
    
    def teardown_environment(self):
        raise NotImplementedError

    def run_tests(self):
        raise NotImplementedError
    
    def report_test(self):
        return ''

    def lifecycle(self):
        test_results = []
        self._logger('Creating New Environment',fg='green')
        self._logger(self.report_test(),fg='green')
        self.failed = False
        self.error_stack_trace = None
        self.error = None
        try:
            self.create_environment()
            self._logger('Environment Created, Now Running Tests',fg='green')
            test_results = self.run_tests()
            self._logger('Finished Running Test ! Wohoo!',fg='green')
        except Exception as e:
            error_string = traceback.format_exc()
            self.failed = True
            self.error = e
            self.error_stack_trace = error_string
            self._logger(f'Something Failed ! {error_string}',fg='red')
        finally:
            self._logger("Tearing down environment",fg='green')
            self.teardown_environment()
        
        return test_results

class MetaflowIntegrationTest(IntegrationTestEnvironment):
    def __init__(self) -> None:
        self._metadataservice_url = None
        self._database_host = None
        self._database_port = None
        self._database_user = None
        self._database_password = None
        self._database_name = None
        self._migrationservice_url = None

    @property
    def metadataservice_url(self):
        return self._metadataservice_url
    
    @property
    def database_name(self):
        return self._database_name

    @property
    def database_host(self):
        return self._database_host

    @property
    def database_port(self):
        return self._database_port
    
    @property
    def database_user(self):
        return self._database_user

    @property
    def database_password(self):
        return self._database_password
    
    @property
    def migrationservice_url(self):
        return self._migrationservice_url

    def create_environment(self):
        # Build the image of the Metadata service.
        self._logger('Creating Postgres Docker Container',fg='blue')
        self.setup_database()
        
        self._logger('Building Metadata Image',fg='blue')
        self.setup_metadata_service()
        time.sleep(5)

    def teardown_database(self):
        raise NotImplementedError
    
    def setup_database(self):
        raise NotImplementedError
    
    def teardown_metadata_service(self):
        raise NotImplementedError
    
    def setup_metadata_service(self):
        raise NotImplementedError

class DockerTestEnvironment(MetaflowIntegrationTest):
    """ 
    Lifecycle :
        create_env 
            --> setup network
            --> setup image of the repo
            --> create database container
            --> create mdservice container from image

        run_tests
            --> MFTestRunner
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
                dont_remove_containers=False,
                build_md_image=False,
                md_docker_image = None,
                database_port = 5432,
                logger=None,
                with_md_logs = True,
                image_name = 'metaflow_metadata_service',
                network_name='postgres-network',\
                image_build_path='../',\
                docker_file_path=os.path.abspath('../Dockerfile')) -> None:
        
        self._docker = docker.DockerClient(base_url='unix://var/run/docker.sock')

        self._logger = logger if logger is not None else lambda *args:print(*args)
        self._with_md_logs = with_md_logs
        self._dont_remove_containers = dont_remove_containers
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
        
        # Configuration related to MD Service Image 
        self._build_md_image = build_md_image
        self.md_docker_image = md_docker_image

        # Configuration related to the containers for the test harness. 
        self._docker_file_path = docker_file_path
        self._image_name = image_name # Image of MD Service
        self._metadataservice_container = None
        self._metadata_image = None
        self._image_build_path = image_build_path
        if self._build_md_image:
            self._md_repo_version = self._loose_git_version()

        self._metadataservice_name = 'testharness-metadataservice'

        # Local Test Harness Related Configuration
        self._flow_dir = flow_dir
        self._mf_versions = versions
        self._temp_env_store = temp_env_store
    
    @property
    def metadataservice_version(self):
        md_version = self.md_docker_image
        if self._build_md_image:
            md_version = f'git:{self._md_repo_version}'
        
        return md_version

    def _loose_git_version(self):
        try:
            import git
            repo = git.Repo(search_parent_directories=True)
            sha = repo.head.object.hexsha
            branch_name = repo.active_branch.name
            return f'{branch_name}-{sha}'
        except ImportError:
            pass
        return None

    def report_test(self):
        md_version = self.metadataservice_version
        return '\n\t'.join(
            ['Running test with parameters : \n\t']+[
            f"{k} : {v}" for k,v in dict(
                Metaflow_Versions = ', '.join(self._mf_versions),
                Metadata_Service_Version = md_version,
            ).items()
        ] + ['\n\t'])
            
    
    def get_tags(self):
        return [
            f"service_version:{self.metadataservice_version}"
        ]
        
        
    def run_tests(self):
        url = f"http://localhost:8080/"
        test_runner = MFTestRunner(
            self._flow_dir,
            EnvConfig(
                tags = self.get_tags(),
                datastore='local',
                metadata='service',
                metadata_url=url
            ),
            versions=self._mf_versions,
            temp_env_store=self._temp_env_store,
        )
        test_results = test_runner.run_tests()
        for res in test_results:
            message = f"Successfully executed flow {res['flow']}/{res['run']} with Metaflow version {res['version']}"
            fg='green'
            if not res['success']:
                message = f"Failed in executing flow {res['flow']}/{res['run']} with Metaflow version {res['version']}"
                fg='red'
            self._logger(message,fg=fg)
        return test_results

    def teardown_environment(self):
        
        if self._with_md_logs:
            md_logs = str(self._metadataservice_container.logs().decode('utf-8'))
            self._logger(f'Metadata Logs ::  \n {md_logs}',fg='blue')
        
        # If we set an arg not remove containers. 
        if not self._dont_remove_containers :
            self._logger('Stopping all containers',fg='blue')
            self.teardown_metadata_service()
            self.teardown_database()
            # Remove the network 
            if self._database_container is None:
                self._logger('Removing Network',fg='blue')
                self._network.remove()

            if self._build_md_image:
                self._logger('Removing Docker Images',fg='blue')
                # remove the images.
                try:
                    self._docker.images.remove(self._metadata_image.id)
                except APIError as e:
                    if e.status_code == 409:
                        self._logger(f'Unable to delete the Metadata image. \n {e.explanation}',fg='red')
            # remove temporary directory of MF versions
            is_present = False
            try: 
                os.stat(self._temp_env_store)
                is_present = True
            except FileNotFoundError as e:
                pass
            if is_present:
                shutil.rmtree(self._temp_env_store)
            try:
                shutil.rmtree('.metaflow')
            except FileNotFoundError: 
                pass
        
    
    def _db_env_vars(self):
        return dict(
            POSTGRES_USER=self.database_user,
            POSTGRES_PASSWORD=self.database_password ,
            POSTGRES_DB=self.database_name
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
        self._logger(f'Using DB Ip Address {self.database_host} ',fg='green')
        return dict(
            MF_METADATA_DB_HOST = self.database_host,
            MF_METADATA_DB_PORT = self.database_port,
            MF_METADATA_DB_USER = self.database_user,
            MF_METADATA_DB_PSWD = self.database_password,
            MF_METADATA_DB_NAME = self.database_name,
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

    def teardown_database(self):
        self._logger('Removing Database related container',fg='blue')
        if self._database_container is not None:
            self._teardown_container(self._database_container)
            self._database_container = None

    def _teardown_container(self,container):
        container.stop(timeout=10)
        container.reload()
        container.remove()
    
    def teardown_metadata_service(self):
        self._logger('Removing MD service related container',fg='blue')
        if self._metadataservice_container is not None:
            self._teardown_container(self._metadataservice_container)
            self._metadataservice_container = None

    def setup_database(self):
        # Create the Postgres container
        self._database_container = self._docker.containers.run(POSTGRES_IMAGE,\
                                            detach=True,\
                                            ports=self._db_ports(),\
                                            name=self._database_container_name,\
                                            environment=self._db_env_vars(),\
                                            network=self._network_name,)
        time.sleep(20)
        self._database_host = self._resolve_ipaddr(self._database_container,wait_time=120)

    def setup_metadata_service(self):
        self._metadata_image = self._build_mdservice_image()
        time.sleep(5)
        self._logger('Creating Metadata Service Container',fg='blue')
        self._metadataservice_url = 'http://localhost:8080'
        self._migrationservice_url = 'http://localhost:8082'
        # Create the metadata service container
        self._metadataservice_container = self._docker.containers.run(self._image_name,\
                                                    detach=True,\
                                                    stdin_open=True,\
                                                    tty=True,\
                                                    environment=self._mdcontainer_env_vars(),\
                                                    network=self._network_name,\
                                                    ports=self._mdservice_ports(),\
                                                    )
        
    def create_environment(self):
        try:
            # If .metaflow is found then remove it 
            # this is done for fresh tests not conflicting with old tests
            os.stat('.metaflow')
            shutil.rmtree('.metaflow')
        except FileNotFoundError:
            pass
        self._logger('Creating a network',fg='blue')
        self._network = self._find_network()
        
        if self._network is None:
            self._network = self._create_network()

        super().create_environment()
        
    
    def _build_mdservice_image(self):
        if self._build_md_image:
            image,log_generator = self._docker.images.build(path=self._image_build_path,\
                                                        dockerfile=self._docker_file_path,\
                                                        tag=self._image_name,)
        else:
            image = self._docker.images.pull(f"{MD_SERVICE_IMAGE_NAME}",tag=self.md_docker_image)
            self._image_name = f"{MD_SERVICE_IMAGE_NAME}:{self.md_docker_image}"
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
