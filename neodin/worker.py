from __future__ import annotations
import logging
from pydantic import BaseModel
from pathlib import Path
import datetime as dt
import subprocess
import io
import random
import time
import fcntl
from typing import Callable
import re
from typing import ClassVar, Iterable
import traceback
import zoneinfo
from hashlib import md5

import ruamel.yaml as yaml


FORMAT = '[%(asctime)s] %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('neodin.worker')
logger.setLevel(logging.DEBUG)


logger.debug('This is a debug message')


class StateLocationConfig(BaseModel):
    working_dir: Path

    @property
    def jobs_dir(self) -> Path:
        return self.working_dir / 'jobs'

    @property
    def done_dir(self) -> Path:
        return self.working_dir / 'done'

    @property
    def failed_dir(self) -> Path:
        return self.working_dir / 'failed'

    @property
    def logs_dir(self) -> Path:
        return self.working_dir / 'logs'

    @property
    def results_dir(self) -> Path:
        return self.working_dir / 'results'

    @classmethod
    def user_home_relative(cls, run_id: str) -> StateLocationConfig:
        return cls(working_dir=Path.home() / '.neodin' / 'state' / run_id)

    def make_dirs(self):
        self.working_dir.mkdir(parents=True, exist_ok=True)
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.done_dir.mkdir(parents=True, exist_ok=True)
        self.failed_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)


class YamlSerde(BaseModel):
    def dump(self, path: Path) -> None:
        with path.open("w") as f:
            f.write(yaml.dump(self.model_dump()))


    @classmethod
    def load(cls, path: Path) -> dict:
        return cls(**yaml.safe_load(path.read_text()))

class JobConfig(BaseModel):
    state_location: StateLocationConfig
    dry_run: bool = False
    command: str
    env: dict[str, str] = {}
    constant_args: dict[str, str] = {}
    variable_args: list[dict[str, str]] = []
    keep_retrying: bool = False

    job_filename_regex: ClassVar[re.Pattern] = re.compile(r"^(?P<job_class_name>.+)_(?P<var_job_args_hash>.+)\.(?P<extension>\w+)$")
    job_instance_filename_regex: ClassVar[re.Pattern] = re.compile(r"^(?P<job_class_name>.+)_(?P<var_job_args_hash>.+)_(?P<timestamp>w+)\.(?P<extension>\w+)$")
    

    job_name_format: ClassVar[str] = "{job_class_name}_{var_job_args_hash}"
    job_extension: ClassVar[str] = ".job.yaml"

    job_instance_name_format: ClassVar[str] = "{job_class_name}_{var_job_args_hash}_{timestamp}"
    job_instance_extension: ClassVar[str] = ".jobinstance.yaml"
    job_instance_result_extension: ClassVar[str] = ".result.yaml"
    job_instance_log_extension: ClassVar[str] = ".log.txt"
    


    def list_jobs(self) -> list[Path]:
        return [job_file_path for job_file_path in state_location.jobs_dir.glob("*.job.yaml") if self.is_job_file(job_file_path)]
    
    def is_job_file(self, file_path: Path) -> bool:
        return self.job_filename_regex.fullmatch(file_path.name) is not None
    
    def var_job_args_hash(self, variable_args: dict[str, str]) -> str:
        return md5(str(variable_args).encode()).hexdigest()

    def create_jobs(self) -> Iterable[Job]:
        for variable_args in self.variable_args:
            yield Job(
                job_class_name=self.__class__.__name__,
                var_job_args=variable_args,
                command=self.get_subprocess_command(variable_args),
            )

    def crete_job_files(self) -> None:
        for job in self.create_jobs():
            job.create_job_file(jobs_path=self.state_location.jobs_dir)


    def get_subprocess_command(self, variable_args) -> SubprocessCommand:
        return SubprocessCommand(command=self.command, args=[f"{name}={value}" for name, value in (self.constant_args | variable_args).items()])


class SubprocessCommand(BaseModel):
    command: str
    args: list[str] = []
    env: dict[str, str] = {}

    def execute(self, dry_run: bool = False, log_file: io.StringIO | None = None) -> int:
        logger.debug(f'Executing command: {self.command} {" ".join(self.args)}')
        if dry_run:
            return 0
        else:
            process = subprocess.Popen([self.command, *self.args], env=self.env, stdout=log_file, stderr=log_file)
            process.communicate()
            return process.returncode

class Job(YamlSerde):
    job_class_name: str
    var_job_args: dict[str, str]
    command: SubprocessCommand

    def get_job_config(self) -> JobConfig:
        return globals()["job_config"]
    
    def create_instance(self, dry_run: bool = False, timestamp_provider: Callable[[], dt.datetime] = dt.datetime.now) -> JobInstance:

        job_config = self.get_job_config()
        job_name = job_config.job_instance_name_format.format(
            job_class_name=self.__class__.__name__, 
            var_job_args_hash=job_config.var_job_args_hash(variable_args=self.var_job_args), 
            timestamp=dt.datetime.now().isoformat()
        )

        return JobInstance(
            log_file_path_str=(job_config.state_location.logs_dir / (job_name + job_config.job_instance_log_extension)).as_posix(),
            results_file_path_str=(job_config.state_location.results_dir / (job_name + job_config.job_instance_result_extension)).as_posix(),
            job=self,
            dry_run=dry_run,
            timestamp=timestamp_provider(),
        )
    def create_job_file(self, jobs_path: Path) -> None:
        job_config = self.get_job_config()
        job_file_path = jobs_path / (job_config.job_name_format.format(
            job_class_name=self.job_class_name,
            var_job_args_hash=job_config.var_job_args_hash(variable_args=self.var_job_args)
        ) + job_config.job_extension)
        self.dump(job_file_path)

class JobInstanceResult(YamlSerde):
    command: SubprocessCommand
    success: bool
    started_at: dt.datetime
    log: str
    run_time_seconds: float

class JobInstance(YamlSerde):
    log_file_path_str: str
    results_file_path_str: str
    job: Job
    dry_run: bool = False
    timestamp: dt.datetime = dt.datetime.now()
    

    def dump(self, jobs_path: Path) -> None:
        job_config_class = self.job.get_job_config_class()
        job_file_path = jobs_path / job_config_class.job_instance_name_format.format(
            job_class_name=self.job.job_class_name,
            var_job_args_hash=self.job.var_job_args,
            timestamp=self.timestamp.isoformat()
        )
        
        self.dump(job_file_path)

    @classmethod
    def from_job_file(cls, job_file_path: Path) -> JobInstance:
        with job_file_path.open("r") as job_file:
            content = job_file.read()
        job = yaml.safe_load(content)
        return cls(**job)

    def execute(self) -> int:
        logger = logging.getLogger(__name__)

        logger.debug(f'STARTING: {self.job.var_job_args}')
        started_at = dt.datetime.now()
        success = False
        log_file_path = Path(self.log_file_path_str)
        result_file_path = Path(self.results_file_path_str)
        try:
            with log_file_path.open("w") as log_file:
                retcode = self.job.command.execute(self.dry_run, log_file)
                success = retcode == 0
                return success
        except Exception as e:
            logger.error(f'Error: {e}', exc_info=e)
        finally:
            logger.debug(f'FINISHED: {self.job.var_job_args}, success={success}')
            JobInstanceResult(
                    command=self.job.command,
                    success=success,
                    started_at=started_at.isoformat(),
                    log=log_file_path.as_posix(),
                    run_time_seconds=(dt.datetime.now() - started_at).total_seconds()
            ).dump(result_file_path)



class JobAlreadyRunningError(Exception):
    ...


class JobFinishingError(Exception):
    ...


def unlock_and_move_job_file(job_path: Path, config: JobConfig, success: bool):
    try:
        with open(job_path, "w") as job_file:
            fcntl.flock(job_file, fcntl.LOCK_UN)

        if success:
            job_path.rename(config.state_location.done_dir / job_path.name)
        else:
            job_path.rename(config.state_location.failed_dir / job_path.name)
    except Exception as e:
        logger.error(f'Error: {e}', exc_info=e)
        raise JobFinishingError(f"Error finishing job {job_path}")


class JobWorker:
    def __init__(self, state_location: StateLocationConfig, job_config: JobConfig):
        self.state_location = state_location
        self.job_config = job_config

    def manage_job_locks(self, job_path: Path, callback: Callable[[], bool]):
        try:
            success = False

            with open(job_path, "w") as job_file:
                fcntl.flock(job_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                success = callback()
            if success or not self.job_config.keep_retrying:
                unlock_and_move_job_file(job_path, config=self.job_config, success=success)
        except BlockingIOError:
            logger.debug(f'Job already running: {job_path}')
            traceback.print_exc()

            raise JobAlreadyRunningError(f"Attempted to run job {job_path} but it is already running.")

    def start_worker(self):
        while True:
            jobs_todo = self.job_config.list_jobs()
            if not jobs_todo:
                logger.warning('No jobs to do. Exiting.')
                exit(0)
            try:
                job_file_path = random.choice(jobs_todo)

            except Exception as e:
                logger.error(f'Error: {e}')

            
            try:
                job = Job.load(job_file_path)
                instance = job.create_instance()
                self.manage_job_locks(job_path=job_file_path,callback=lambda: instance.execute())
            except JobAlreadyRunningError:
                logger.debug(f'Job already running: {job_file_path}')
                time.sleep(1)

class ConcreteJobConfig(JobConfig):
    env_type: str

state_location = StateLocationConfig.user_home_relative('test')
state_location.make_dirs()
job_config = ConcreteJobConfig(env_type='prod',
    command="echo", 
    state_location=state_location,
    constant_args=dict(to_print='dupa'), 
    variable_args=[
        dict(aaa='1'), 
        dict(aaa='2'),
    ],
)


if __name__ == '__main__':
    job_config.crete_job_files()

    worker = JobWorker(state_location=state_location, job_config=job_config)
    worker.start_worker()
