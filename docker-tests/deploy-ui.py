import click
import docker
from docker.errors import NotFound
import tempfile
from env_buider import IpNotResolved
import time
import os
from functools import partial
import datetime
POSTGRES_IMAGE = 'postgres:latest'
MF_UI_CLONE_REPO = 'git@github.com:Netflix/metaflow-ui.git'
MD_IMAGE_NAME = 'metaflow_metadata_service'

TIME_FORMAT = "%Y-%m-%d %I:%M:%S %p"

def logger(base_logger,*args,**kwargs):
    msg = f'{datetime.datetime.now().strftime(TIME_FORMAT)} - Metaflow Integration Test Harness - {args[0]}'
    base_logger(msg,**kwargs)


class NetworkNotFound(Exception):
    headline = 'Cannot Find Network'
    def __init__(self, network_name=''):
        self.message = f"Cannot Find Network : {network_name}"
        super(NetworkNotFound, self).__init__()


class DeployUi:
    def __init__(self,
                database_name='metaflow',
                database_password='password',
                database_user='metaflow',
                database_port = 5432,
                logger=partial(logger,click.secho),
                version='0.17.0',
                docker_file_path=os.path.abspath('../Dockerfile.ui_service'),
                docker_context=os.path.abspath('../'),
                network_name='postgres-network') -> None:
        self._docker = docker.DockerClient(base_url='unix://var/run/docker.sock')
        self._ui_service_docker_file_path = docker_file_path
        self._ui_service_image_context = docker_context
        native_logger = lambda x,**kwargs : print(x,**kwargs)
        self._logger = native_logger
        if logger is not None:
            self._logger = logger
        self._max_ip_wait_time = 100

        self._version = version
        self._network_name = network_name
        self._tempdir = tempfile.TemporaryDirectory(prefix='mf-gui')
        self.database_name = database_name
        self.database_password = database_password
        self.database_user = database_user
        self.database_port = database_port
        self._md_service_url = None
        self._ui_image = None
        self._ui_image_name = None

    def deploy(self):
        self._md_service_url = self._create_ui_service_container()
        self._logger(f"Found MD Service URL : {self._md_service_url} ",fg='green')
        dockerfile_path = self._clone_ui_repo(self._version,self._tempdir.name)
        self._logger(f"Cloned Ui Repo & now building docker image",fg='green')
        self._ui_image, self._ui_image_name = self._build_ui_image(dockerfile_path)
        self._logger(f"Deploying Ui Container ",fg='green')
        self._ui_container = self._deploy_ui_container()
        self._logger(f"Running The Ui Container : {self._ui_container.id}",fg='green')

    def _ui_env_vars(self):
        return {
            "METAFLOW_SERVICE":self._md_service_url
        }
    def _ui_ports(self):
        return {
            '3000/tcp':3000,
        }
    def _find_network(self):
        try:
            return self._docker.networks.get(self._network_name)
        except NotFound as e:
            return None
        except:
            raise

    
    def _deploy_ui_container(self):
        network = self._find_network()
        if network is None:
            raise NetworkNotFound(network_name=self._network_name)
        
        return self._docker.containers.run(self._ui_image_name,\
                                        detach=True,\
                                        stdin_open=True,\
                                        tty=True,\
                                        environment=self._ui_env_vars(),\
                                        network=self._network_name,\
                                        ports=self._ui_ports(),\
                                        )


    def _build_ui_image(self,dockerfilepath):
        tag = f'metaflow-ui:{self._version}'
        image,log_generator = self._docker.images.build(path=self._tempdir.name,\
                                                        dockerfile=dockerfilepath,\
                                                        tag=tag,)        

        return image, tag

    @staticmethod
    def _clone_ui_repo(ui_version,dir):
        tag = ui_version
        if str(ui_version).lower() in ['main', 'latest','master','head']: 
            tag = 'master'
        from git import Repo

        cloned_repo = Repo.clone_from(MF_UI_CLONE_REPO, dir)
        cloned_repo.heads[tag].checkout()
        return os.path.join(dir,'Dockerfile')

    def _find_db_container(self):
        containers = self._docker.containers.list(filters=dict(ancestor=POSTGRES_IMAGE))
        for cont in containers:
            self._logger(f"Found Container {cont.image} {cont.name}",fg='blue')
        if len(containers) ==0:
            return None
        return containers[0]
        

    def _create_ui_service_container(self):
        self._logger(f"Creating the UI service container. ",fg='blue')
        db_container = self._find_db_container()
        if db_container is None:
            raise Exception("MD Container Not found")
        db_ip_addr = self._resolve_ipaddr(db_container)
        self._logger(f"Using Postgres on IP {db_ip_addr}",fg='blue')
        tag = f'metaflow-ui-service'
        image,log_generator = self._docker.images.build(path=self._ui_service_image_context,\
                                                        dockerfile=self._ui_service_docker_file_path,\
                                                        tag=tag,)
        self._logger(f"Built the UI service Image ",fg='blue')
        md_vars = dict(
            MF_METADATA_DB_HOST = db_ip_addr,
            MF_METADATA_DB_PORT = self.database_port,
            MF_METADATA_DB_USER = self.database_user,
            MF_METADATA_DB_PSWD = self.database_password,
            MF_METADATA_DB_NAME = self.database_name,
        )
        self._ui_service_container = self._docker.containers.run(image.id,\
                                                    detach=True,\
                                                    stdin_open=True,\
                                                    tty=True,\
                                                    environment=md_vars,\
                                                    network=self._network_name,\
                                                    ports={"8083/tcp":8083},\
                                                    )
        self._logger(f"UI Service Container Started",fg='blue')
        
        # return f'http://{md_ip_addr}:8080'
        return f'http://localhost:8083'
    
    def _resolve_ipaddr(self,container,wait_time=None):
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
        

@click.command()
@click.option('--database-password',default="ByvI)Sr_uamaPx$w&Xp_LoB*DVBzTO+3oK{Z_Nw4SRcxut?-B>h]&WD}_mU!AgOm'\"")
@click.option('--database-name',default='metaflow')
@click.option('--database-user',default='metaflow')
@click.option('--database-port',default=5432)
@click.option('--version',help='Version of the UI to deploy')
def deploy_ui(version='0.17.0',\
            database_name='metaflow', \
            database_password=None, \
            database_user='metaflow', \
            database_port=5432, 
            ):
    ui_deployer = DeployUi(\
        version=version,
        database_name = database_name,
        database_password = database_password,
        database_user = database_user,
        database_port = database_port,
    )
    ui_deployer.deploy()


if __name__ == "__main__":
    deploy_ui()