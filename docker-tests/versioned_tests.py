# This is the a test runner for various metaflow versions. It will run test cases with different versions of the metaflow clients. 
# Very barebone; Needs polishing to make it very efficient and customizable. 
import multiprocessing
import os
import glob
from sys import version
import venv
import uuid
import shutil
import subprocess 
from multiprocessing import Process
import logging
import click
import time
import re
import json
WORKFLOW_EXTRACT_REGEX = re.compile('\(run-id (?P<runid>[a-zA-Z0-9_-]+)',re.IGNORECASE)
FLOW_EXTRACTOR_REGEX = re.compile('^(\S+) (\S+) (\S+) (?P<flow>[A-Za-z0-9_]+) (\S+) (\S+)',re.IGNORECASE)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_json(pth):
    with open(pth,'r') as f:
        return json.load(f)

def save_json(data,pth):
    with open(pth,'w') as f:
        json.dump(data,f)

METAFLOW_VERSIONS = [
    "2.0.1",
    "2.0.2",
    "2.0.3",
    "2.0.4",
    "2.0.5",
    "2.1.0",
    "2.1.1",
    "2.2.0",
    "2.2.1",
    "2.2.2",
    "2.2.3",
    "2.2.4",
    "2.2.5",
    "2.2.6",
    "2.2.7",
    "2.2.8",
    "2.2.9",
    "2.2.10",
    "2.2.11",
    "2.2.12",
    "2.2.13",
    "2.3.0",
    "2.3.1",
    "2.3.2",
    "2.3.3",
    "2.3.4",
    "2.3.5",
    "2.3.6",
    "2.4.0",
]
def create_logger(logger_name:str,level=logging.INFO):
    custom_logger = logging.getLogger(logger_name)
    ch1 = logging.StreamHandler()
    ch1.setLevel(level)
    ch1.setFormatter(formatter)
    custom_logger.addHandler(ch1)
    custom_logger.setLevel(level)    
    return custom_logger


def is_present(pth):
    try:
        os.stat(pth)
        return True 
    except:
        return False

class EnvConfig:
    def __init__(self,\
                datastore='local',\
                s3_datastore_root=None,\
                s3_datatools_root=None,\
                tags = [],\
                metadata='local',\
                metadata_url=None) -> None:
        self.datastore= datastore
        self.tags =tags
        self.s3_datastore_root= s3_datastore_root
        self.metadata= metadata
        self.metadata_url= metadata_url
        self.s3_datatools_root= s3_datatools_root
        self._validate()
    
    def _validate(self):
        if self.datastore == 's3':
            assert self.s3_datastore_root is not None
            assert self.s3_datatools_root is not None
        if self.metadata == 'service':
            assert self.metadata_url is not None and self.metadata != 'local'
        
    
    def get_env(self):
        env_dict = {}
        if self.datastore == 'local':
            env_dict['METAFLOW_DEFAULT_DATASTORE'] = 'local'
        else:
            env_dict['METAFLOW_DEFAULT_DATASTORE'] = 's3'
            env_dict['METAFLOW_DATASTORE_SYSROOT_S3']= self.s3_datastore_root
            env_dict['METAFLOW_DATATOOLS_S3ROOT']= self.s3_datastore_root
        
        if self.metadata == 'local':
            env_dict['METAFLOW_DEFAULT_METADATA'] = 'local'
        else:
            env_dict['METAFLOW_DEFAULT_METADATA'] = 'service'
            env_dict['METAFLOW_SERVICE_URL'] = self.metadata_url
        return env_dict

class TestEnvironment:
    # this will create a session id
    def __init__(self,version_number,temp_dir_name,env_config:EnvConfig):
        self.session_id =self.session_id_hash(version_number)
        self.version_number=version_number
        self.parent_dir = temp_dir_name
        self.env_path = os.path.join(temp_dir_name,self.session_id)
        self.env_config = env_config
        self.python_path = os.path.join(
            self.env_path,
            'bin',
            'python'
        )
        self.pip_path = os.path.join(
            self.env_path,
            'bin',
            'pip'
        )
    
    @staticmethod
    def session_id_hash(version_number):
        return hex(hash(version_number))

    def execute_flow(self,file_pth,batch=False,):
        cmd = [
            self.python_path,
            file_pth,
            'run',
        ]
        if batch:
            cmd+=['--with','batch']
        env = {}
        env.update({k: os.environ[k] for k in os.environ if k not in env})
        env.update(self.env_config.get_env())
        env["PYTHONPATH"] = self.python_path
        if len(self.env_config.tags) > 1:
            for t in self.env_config.tags:
                cmd.extend(['--tag',t])

        run_response,fail = self._run_command(cmd,env)
        return dict(success=not fail,**self._get_runid(run_response))
       

    def _get_runid(self,run_response):
        # Todo Improve ways to get the runID's
        lines = run_response.decode('utf-8').split('\n')
        # print(lines)
        flow,runid=None,None
        try:
            runidstr=run_response.decode('utf-8').split(' Workflow starting ')[1]
            datadict = WORKFLOW_EXTRACT_REGEX.match(runidstr).groupdict()
            runid = datadict['runid']
            flow = FLOW_EXTRACTOR_REGEX.match(lines[0]).groupdict()['flow']
        except IndexError:
            pass
        return dict(
            flow=flow,
            run=runid,
            logs = run_response.decode('utf-8')
        )

    
    def _run_command(self,cmd,env):
        fail = False
        try:
            rep = subprocess.check_output(
                cmd,
                env=env,stderr=subprocess.STDOUT,
            )
        except subprocess.CalledProcessError as e:
            rep = e.output
            fail=True
        return rep,fail

    def to_dict(self):
        return dict(
            env_path=self.env_path,
            version = self.version_number,
        )
    
    def __enter__(self):
        env_builder = venv.EnvBuilder(with_pip=True)
        env_builder.create(self.env_path)
        # Install verion of Metaflow over here. 
        pip_install_cmd = [
            self.pip_path,
            'install',
            f'metaflow=={self.version_number}'
        ]
        env = {}
        env.update({k: os.environ[k] for k in os.environ if k not in env})
        env["PYTHONPATH"] = self.python_path
        self._run_command(pip_install_cmd,env)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        shutil.rmtree(self.env_path)


class FlowInstanceTest:
    # this should use the TestEnvironment and run the actual test
    def __init__(self,version_number,flow_files,temp_dir,envionment_config) -> None:
        # Process.__init__(self,daemon=False,) # Making it a deamon process. 
        self.test_id = str(uuid.uuid4())[:4]
        self.version_number,self.flow_files = version_number,flow_files
        self.temp_dir=temp_dir
        self._version = version_number
        self.envionment_config = envionment_config
        self.logger = create_logger(self.logger_name)
    
    @property
    def logger_name(self):
        return f'FlowInstanceTest-{self.test_id}-{self._version}'
    
    @property
    def saved_file_name(self):
        return f'{self.logger_name}.json'
    
    def metadata(self):
        import json
        return f"""
        Metaflow Version : {self.version_number}
        Configuration Variables: 
        
        {json.dumps(self.envionment_config.get_env(),indent=4)}
        """

    def run(self):
        """run 
        """
        with TestEnvironment(self.version_number,self.temp_dir,self.envionment_config) as env:
            test_res_data = []
            for file in self.flow_files:
                # env.execute_flow should be blocking. 
                flow_exec_resp = env.execute_flow(file)
                env_info = env.to_dict()
                env_info.update(flow_exec_resp)
                env_info.update(dict(file_name=file))
                test_res_data.append(env_info)
                # Todo : Create script to manipulate the run flow
                # todo : Manage Local datastore/metadata
                self.logger.debug(f"Ran Flow File : {file} On Version : {self.version_number}")
            self.logger.debug(f"Saving File : {file} On Version : {self.version_number}")
            filename = self.saved_file_name
            save_json(test_res_data,filename)
            return filename


def run_test(version_number,flow_files,temp_dir,envionment_config):
    return FlowInstanceTest(
        version_number,flow_files,temp_dir,envionment_config
    ).run()

class MFTestRunner:

    def __init__(self,\
                flow_dir,\
                envionment_config:EnvConfig,
                max_concurrent_tests= 2,\
                versions=METAFLOW_VERSIONS,\
                temp_env_store='./tmp_verions') -> None:
        self.flow_files = glob.glob(os.path.join(flow_dir,"*_testflow.py"))
        self.versions = versions
        self._max_concurrent_tests = max_concurrent_tests
        self.envionment_config = envionment_config
        # Todo : figure test concurrency
        # todo assert versions are the same as `METAFLOW_VERSIONS`
        self.temp_env_store = temp_env_store
        assert not is_present(temp_env_store),"temp directory should be empty"
        os.makedirs(self.temp_env_store)
        assert len(self.flow_files) > 0, "Provide a directory with *_testflow.py as files; These files should contain metaflow flows"
    
    def _make_tests(self):
        return [(version,self.flow_files,self.temp_env_store,self.envionment_config) \
                for version in self.versions]

    def run_tests(self):
        # create a session Id for each test
        # Make a virtual environment in the same name in temp dir
        tests = self._make_tests()
        results = []
        for test in tests:
            try:
                p = run_test(*test)
                results.extend(load_json(p))
            except Exception as e:
                print(e)
        shutil.rmtree(self.temp_env_store)
        return results


# def run_tests():
#     test_runner = MFTestRunner('./test_flows',EnvConfig(),versions=METAFLOW_VERSIONS,)
#     test_runner.run_tests()

# if __name__ == '__main__':
#     run_tests()