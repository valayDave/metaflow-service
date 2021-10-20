import json
import os
import time
import requests
from env_buider import DockerTestEnvironment, METAFLOW_VERSIONS, POSTGRES_IMAGE 
# (version_number, migration_required, use_local)
METADATA_MIGRATION_VERSIONS = [
    ("1.0.0",False, False),
    ("1.0.1",False, False),
    ("2.0.0",False, False),
    ("2.0.1",True, False),
    ("2.0.2",True, False),
    ("2.0.3",True, False),
    ("2.0.4",True, False),
    ("2.0.5",True, False),
    ("2.0.6",True, False),
    ("latest",True, False),
    (None, True, True),
]

class MigrationFailingException(Exception):
    headline = 'Migration API Failed'
    def __init__(self, md_service_version='',error=None,):
        self.message = f"MD Service Failed with version {md_service_version} : {error} "
        super(MigrationFailingException, self).__init__()
    
    def __str__(self) -> str:
        return f'{self.headline}\n\t{self.message}'


class FixedDatabaseEnvironment(DockerTestEnvironment):
    def __init__(self, \
                run_migration=False,\
                versions=METAFLOW_VERSIONS,\
                flow_dir='./test_flows',\
                temp_env_store='./tmp_verions',
                max_ip_wait_time=20, database_name='metaflow', 
                database_password='password', 
                database_user='metaflow', 
                dont_remove_containers=False, 
                build_md_image=False, 
                md_docker_image=None, 
                database_port=5432, 
                logger=None, 
                with_md_logs=True, 
                image_name='metaflow_metadata_service', 
                network_name='postgres-network', 
                image_build_path='../', 
                docker_file_path=os.path.abspath('../Dockerfile')) -> None:
        self.run_migration = run_migration
        self.migration_response = None
        self.db_version = "0"
        super().__init__(versions=versions, 
                        flow_dir=flow_dir, 
                        temp_env_store=temp_env_store, 
                        max_ip_wait_time=max_ip_wait_time, 
                        database_name=database_name, 
                        database_password=database_password, 
                        database_user=database_user, 
                        dont_remove_containers=dont_remove_containers, 
                        build_md_image=build_md_image, md_docker_image=md_docker_image, 
                        database_port=database_port, 
                        logger=logger, with_md_logs=with_md_logs, 
                        image_name=image_name, 
                        network_name=network_name, 
                        image_build_path=image_build_path, 
                        docker_file_path=docker_file_path)

    
    def _teardown_database(self):
        # Don't do anything to teardown database. 
        pass

    def get_tags(self):
        if self.db_version is not None:
            return super().get_tags() + [f'db_version:{self.db_version}']

    def destroy_database(self):
        super()._teardown_database()
    
    def _setup_database(self):
        # todo : Search for a postgres container if doesn't exist create one. 
        db_container = self._find_db_container() 
        if db_container is None:
            super()._setup_database()
        else:
            self._database_container = db_container
        self._logger(f"Using DB container ID : {self._database_container.id}",fg='blue')
    
    def _find_db_container(self):
        containers = self._docker.containers.list(filters=dict(ancestor=POSTGRES_IMAGE))
        for cont in containers:
            self._logger(f"Found Container {cont.image} {cont.name}",fg='blue')
        if len(containers) ==0:
            return None
        return containers[0]

    def _setup_metadata_service(self):
        # this will setup the MD container
        super()._setup_metadata_service()
        time.sleep(10)
        self._perform_migration()

    def _perform_migration(self):
        if self.run_migration:
            self.migration_response = self._check_migrations()
            # Store dbversion this as a part of the final test results. 
            self.db_version = self.migration_response['current_version']
            self._logger("Migration response successfull",fg='green')
            # Run the migration over here if the db is not up to date
            if not self.migration_response['is_up_to_date']:
                self._logger("Running schema update",fg='green')
                self._run_schema_upgrade()
                timeoutval = 30
                self._logger(f"Sleeping of {timeoutval} seconds",fg='green')
                time.sleep(timeoutval)
                self._logger("Current Migration Status Post Upgrade Call",fg='yellow')
                self.migration_response = self._check_migrations()
                self.db_version = self.migration_response['current_version']
                self._logger(json.dumps(self._check_migrations(),indent=4),fg='yellow')
                # Restart the container if we run a migration. 
                self._metadataservice_container.restart(timeout=10)
                
    def _check_migrations(self):
        """[summary]
        Migration Service Response:
        {
            "is_up_to_date": true, 
            "current_version": "20201002000616", 
            "migration_in_progress": false, 
            "db_schema_versions": ["1", "20200603104139", "20201002000616"], 
            "unapplied_migrations": []
        }

        Raises:
            MigrationFailingException: [description]

        Returns:
            [type]: [description]
        """
        schema_status_api = "http://localhost:8082/db_schema_status"
        resp = requests.get(schema_status_api)
        if resp.status_code !=200:
            # this Means something Failed. 
            version = self.metadataservice_version
            raise MigrationFailingException(md_service_version=version,error=resp.text)
        return resp.json()
    
    def _run_schema_upgrade(self):
        """

        Raises:
            MigrationFailingException: [when migration service error's out]
        """
        schema_status_api = "http://localhost:8082/upgrade"
        resp = requests.patch(schema_status_api)
        if resp.status_code !=200:
            # this Means something Failed. 
            version = self.metadataservice_version
            raise MigrationFailingException(md_service_version=version,error=resp.text)


def execute_sequential_test(metadata_config=METADATA_MIGRATION_VERSIONS,**kwargs):
    environments = []
    final_results = []
    for version_number, migration_required, use_local in metadata_config:
        environments.append(
            FixedDatabaseEnvironment(
                run_migration=migration_required,
                md_docker_image=version_number,
                build_md_image=use_local,
                **kwargs
            )
        )

    for env in environments:
        data_dict = dict(
            service_version='HEAD',
            results=[],
            run_migration = [],
            migration_status_results = None,
            db_version = None,
            failed = False,
            error_stack_trace = None,
            error = None
        )
        
        data_dict['results'] = env.lifecycle()
        data_dict['service_version'] = env.metadataservice_version
        data_dict['failed'] = env.failed
        data_dict['error_stack_trace'] = env.error_stack_trace
        data_dict['error'] = str(env.error)
        data_dict['db_version'] = env.db_version
        data_dict['run_migration'] = env.run_migration
        data_dict['migration_status_results'] = env.migration_response
        final_results.append(data_dict)
    
    env.destroy_database()

    return final_results