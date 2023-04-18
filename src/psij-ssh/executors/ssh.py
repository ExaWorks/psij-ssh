"""This module contains the SSHJobExecutor :class:`~psij.JobExecutor`."""

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

    def __init__(
        self, url: Optional[str] = None, config: Optional[JobExecutorConfig] = None
    ) -> None:
        """
        Initializes a `SSHJobExecutor`.  It will establish an ssh connection to
        the target host, bootstrap a PSIJ-REST or PSIJ-ZMQ service endpoint
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

        # FIXME: remove
        ve = '/home/merzky/projects/exaworks/psij-ssh/ve3/'

        env_script = ru.which('radical-utils-env.sh')
        ve_script = ru.which('radical-utils-create-ve')

        # FIXME: host, port, auth
        Connection(url.host).run('mkdir -p /tmp/ssh_test/')

        # this is not stable on concurrent client ops
        try:
            Connection('localhost').put(env_script, remote='/tmp/ssh_test/')
        except Exception:
            pass

        try:
            Connection('localhost').put(ve_script, remote='/tmp/ssh_test/')
        except Exception:
            pass

        # cmd = '%s -P "PATH=$PATH:/tmp/ssh_test" -m psij-zmq -v 3.8' % ve_script
        cmd = '%s -P "PATH=$PATH:/tmp/ssh_test" -v 3.8 -t %s' % (ve_script, ve)
        result = Connection('localhost').run(cmd, hide=True)
        # print(result)

        cmd = '. %s/ru_ve_activate.sh; radical-utils-service -n test -c "psij_zmq_service.py"' % ve
        result = Connection('localhost').run(cmd, timeout=5.1, hide=True)
        addr = result.stdout.split(':', 1)[1].strip()
        print(addr)
        ex = JobExecutor.get_instance(name='zmq', url=addr)
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
