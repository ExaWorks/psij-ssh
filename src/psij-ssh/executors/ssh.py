"""This module contains the SSHJobExecutor :class:`~psij.JobExecutor`."""

import os
import threading
from typing import Optional, List, Dict, Any

from psij import (
    Job,
    JobExecutorConfig,
    JobState,
    JobStatus,
    JobExecutor,
    Export,
)

import radical.utils as ru


class SSHJobExecutor(JobExecutor):
    """A :class:`~psij.JobExecutor` for SSH endpoints.

    This executor forwards all requests to an ssh daemon on a remote endpoint
    which is then executing the respective request on the target resource, in
    a shell environment.
    """

    def __init__(self, url: Optional[str] = None,
                 config: Optional[JobExecutorConfig] = None) -> None:
        """
        Initializes a `SSHJobExecutor`.  It will establish an ssh connection to
        the target host, bootstrap a PSIJ-ZMQ or PSIJ-REST service endpoint
        there, and then proxy job submission via that service instance.

        :param url: address at which to contact the remote service.
                    Supported schemas: `ssh+zmq://` and `ssh+rest://`
        :param config: The `SSHJobExecutor` does not have any
                    configuration options.
        """
        ru_url = ru.Url(url)
        if 'ssh+' not in ru_url.schema:
            raise ValueError('expected `ssh+XXX://` type url schema')

        schema = ru_url.schema.split('+', 1)[1]
        if schema not in ['zmq', 'rest']:
            raise ValueError('expected `ssh+zmq://` or `ssh+rest://` url')

        if not config:
            config = JobExecutorConfig()

        super().__init__(url=str(ru_url), config=config)
        self._executor = self._connect_service(ru_url, schema)

    def _connect_service(self, url: ru.Url, schema: str) -> JobExecutor:

        from fabric import Connection

        ve = '/home/merzky/projects/exaworks/psij-ssh/ve3/'  # FIXME: remove
        home = Connection(url.host).run('echo $HOME', hide=True).stdout.strip()
        base = '%s/.psij/' % home
        host = url.host

        env_script_name = 'radical-utils-env.sh'
        ve_script_name = 'radical-utils-create-ve'

        env_script = ru.which(env_script_name)
        ve_script = ru.which(ve_script_name)

        assert env_script
        assert ve_script

        # FIXME: host, port, auth
        Connection(url.host).run('mkdir -p %s' % base)

        # stage the required bootstrap scripts to remote
        # NOTE: this is not stable on concurrent client ops
        Connection(host).put(env_script, remote=base)
        Connection(host).put(ve_script, remote=base)

        cmd = 'chmod 0700 %s/%s %s/%s' % (base, ve_script_name,
                                          base, env_script_name)
        Connection(url.host).run(cmd, hide=True)

        cmd = '/bin/sh %s/%s -P "PATH=$PATH:%s" -v 3.8' \
              % (base, ve_script_name, base)
        cmd += ' -t %s' % ve  # FIXME
        if schema == 'zmq':
            cmd += ' -m psij-zmq'
        elif schema == 'rest':
            cmd += ' -m psij-rest'

        result = Connection(host).run(cmd, hide=True)

        activate = None
        for line in result.stdout.split('\n'):
            if line.startswith('VE_ACTIVATE: '):
                activate = line.split(':', 1)[1].strip()
                break

        activate = '%s/bin/activate' % ve  # FIXME

        cmd = '. %s' % activate
        cmd += '; radical-utils-service -n psij_%s -b %s' % (schema, base)
        if schema == 'zmq':
            cmd += ' -c psij_zmq_service.py'
        elif schema == 'rest':
            cmd += ' -c "psij_rest_service.py 8080"'

        result = Connection(host).run(cmd, timeout=5.1, hide=True)

        addr = None
        if schema == 'zmq':
            line_1 = result.stdout.split('\n', 1)[0]
            addr = line_1.split(':', 1)[1].strip()
        elif schema == 'rest':
            for line in result.stdout.split('\n'):
                if line.startswith('INFO:     Uvicorn running on'):
                    addr = line.split()[4]

        assert addr

        ex = JobExecutor.get_instance(name=schema, url=addr)
        return ex

    def submit(self, job: Job) -> None:
        """See :func:`~psij.job_executor.JobExecutor.submit`."""
        return self._executor.submit(job)

    def cancel(self, job: Job) -> None:
        """See :func:`~psij.job_executor.JobExecutor.cancel`."""
        return self._executor.cancel(job)

    def list(self) -> List[str]:
        """See :func:`~psij.job_executor.JobExecutor.list`.

        Return a list of ids representing jobs that are running on the
        underlying implementation.  We consider the remote service's job ids as
        native job ids and return them unaltered.

        :return: The list of known job ids.
        """
        return self._executor.list()

    def attach(self, job: Job, native_id: str) -> None:
        """
        Attaches a job instance to an existing job.

        The job instance must be in the :attr:`~psij.JobState.NEW` state.

        :param job: The job instance to attach.
        :param native_id: The native ID of the backend job to attached to, as
          obtained through the `:func:list` method.
        """
        return self._executor.attach(job, native_id)
