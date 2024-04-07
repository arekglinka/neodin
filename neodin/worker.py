from __future__ import annotations

import logging
from pydantic import BaseModel
from pathlib import Path
import ruamel.yaml as yaml
import datetime as dt            
import subprocess
import io
import random

import time
import fcntl

from typing import Callable

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
        return cls(working_dir=Path.home() / '.neodin' / 'state'/ run_id)

    def make_dirs(self):
        self.working_dir.mkdir(parents=True, exist_ok=True)
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.done_dir.mkdir(parents=True, exist_ok=True)
        self.failed_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)

import re

class AbstractJobConfig(BaseModel):
    state_location: StateLocationConfig
    keep_retrying: bool = False
    dry_run: bool = False
    variable_job_args: dict[str, str] = {}
    constant_job_args: dict[str, str] = {}
    
    @classmethod
    def is_job_file(cls, job_file_path: Path) -> bool:
        return job_file_path.suffix == '.job'

    @property
    def job_id(self) -> str:
        return re.sub(r'\W+', '-', self.name_format)
    
    @property
    def name_format(self) -> str:
        job_class_name = self.__class__.__name__
        var_job_args = "-".join(f"{name}_{value}" for name, value in self.variable_job_args.items())
        return  f"{job_class_name}_{var_job_args}.job"

    def create_job_file(self, job_id: str, jobs_path: Path) -> AbstractJobConfig:
        return 

    def get_subprocess_command() -> SubprocesCommand:
        return SubprocesCommand(command='echo', args=['Hello, World!'])

    @classmethod
    def from_job_file(cls, job_file_path: Path) -> AbstractJobConfig:
        with job_file_path.open("r") as job_file:
            job = yaml.safe_load(job_file)
        return cls(**job)
    
    @classmethod
    def execute(cls, job_file_path: Path) -> int:
        logger.debug(f'STARTING: {job_file_path}')
        started_at = dt.datetime.utcnow()
        job = cls.from_job_file(job_file_path)
        job.state_location.make_dirs()
        success = False
        log_file_path = job.state_location.logs_dir / f"{job.job_id}.log"
        try:
            with log_file_path as log_file:
                retcode = job.get_subprocess_command().execute(job.dry_run, log_file)
                success = retcode == 0
                return success
        except Exception as e:
            ...
        finally:    
            logger.debug(f'FINISHED: {job_file_path}, success={success}')
            with job.state_location.results_dir / f"{job.job_id}.result" as result_file:
                yaml.dump(dict(
                    job_id=job.job_id,
                    success=success
                    started_at=started_at.isoformat(),
                    log=log_file_path.as_posix(),
                    run_time_seconds=(dt.datetime.utcnow() - started_at).total_seconds()
                ), result_file)

class SubprocesCommand(BaseModel):
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
    
class ConcreteJobConfig(AbstractJobConfig):
    env_type: str


from typing import Generic, TypeVar

T = TypeVar('T', bound=AbstractJobConfig)

class JobFactory(Generic[T], BaseModel):
    job_class: type[AbstractJobConfig]
    constant_job_args: dict[str, str] = {}
    variable_job_args: list[dict[str, str]] = []


    def create_job(self, job_id: str, **kwargs) -> T:
        return T(job_id=job_id, job_template=self, **kwargs)
    
def list_jobs(state_location: StateLocationConfig, job_class: type[AbstractJobConfig]) -> list[Path]:
    return [job_file_path for job_file_path in state_location.jobs_dir.glob("*.job") if job_class.is_job_file(job_file_path)]

class JobAlreadyRunningError(Exception):
    ...

class JobFinishingError(Exception):
    ...

def unlock_and_move_job_file(job_path: Path, config: AbstractJobConfig):
    job = config.from_job_file(job_path)
    try:
        with open(job_path, "w") as job_file:
            fcntl.flock(job_file, fcntl.LOCK_UN)

        if job.success:
            job_path.rename(job.state_location.done_dir / job_path.name)
        else:
            job_path.rename(job.state_location.failed_dir / job_path.name)
    except Exception as e:
        logger.error(f'Error: {e}', exc_info=e)
        raise JobFinishingError(f"Error finishing job {job_path}")

class JobWorker:
    def __init__(self, state_location: StateLocationConfig, job_class: type[AbstractJobConfig]):
        self.state_location = state_location
        self.job_class = job_class

    def manage_job_locks(self, job_path: Path, callback: Callable[[], bool]):
        try:
            success = False
            with open(job_path, "w") as job_file:
                fcntl.flock(job_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                success = callable()
            if success or not self.job_class.keep_retrying:
                unlock_and_move_job_file(job_path, config=config)
        except BlockingIOError:
            logger.debug(f'Job already running: {job_path}')
            import traceback
            traceback.print_exc()
            raise JobAlreadyRunningError(f"Attempted to run job {job_path} but it is already running.")

    def start_worker(self):
        while True:
            try:
                job_file_path = random.choice(list_jobs(self.state_location, self.job_class))
            except Exception as e:
                logger.error(f'Error: {e}')
            
            try:
                self.job_class.execute(job_file_path)
            except JobAlreadyRunningError:
                logger.debug(f'Job already running: {job_file_path}')
                time.sleep(1)