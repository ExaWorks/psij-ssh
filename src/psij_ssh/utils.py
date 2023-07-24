
import os
import fabric

from typing import Optional, List, Tuple

from psij import JobExecutorConfig

import radical.utils as ru

HIDE = True


class Connection(object):
    """base class for `SSHConnection` and `LocalConnection`
    """
    def __init__(self, url: str,
                 config: JobExecutorConfig) -> None:
        """
        Initializes ssh helper.

        :param url: address at which to contact the remote sshd.
                    Supported schemas: `ssh://`
        :param config: AAA options
        """
        self._url = ru.Url(url)
        self._base = str(config.work_directory)
        self._activate = None  # in case no ve is bootstrapped

    def bootstrap(self, rem_path: str = None,
                  modules: Optional[List[str]] = None) -> str:

        raise NotImplementedError('`bootstrap` is not implemented')

    def run(self, cmd: str, timeout: Optional[float] = None
            ) -> Tuple[int, str, str]:
        """
        run the given command over the connection, and return exit code, stdout
        and stderr.
        """
        raise NotImplementedError('`bootstrap` is not implemented')

    def get_tunnel(self, rem_port: int, loc_port: Optional[int]) -> None:
        """
        Create a tunnel using the existing connection, from the remote port
        `rem_port` to the local port `loc_port`.  If the latter is not given,
        a random port is assigned.
        """
        raise NotImplementedError('`bootstrap` is not implemented')


class LocalConnection(Connection):
    """A helper class to manage local endpoints.
    """

    def __init__(self, url: str,
                 config: JobExecutorConfig) -> None:
        """
        Initializes ssh helper.

        :param url: address at which to contact the remote sshd.
                    Supported schemas: `ssh://`
        :param config: AAA options
        """

        super().__init__(url, config)

        if self._url.schema not in ['local', 'fork']:
            raise ValueError('unexpected as url schema [%s]' % self._url.schema)

        if not ru.is_localhost(self._url.host):
            raise ValueError('url host [%s] is not localhost' % self._url.host)

        self._home = os.environ.get('HOME', '/tmp')

        if not self._base:
            self._base = self._home + '/' + '.psij'

        ru.rec_makedir(self._base)

    def bootstrap(self, rem_path: str = None,
                  modules: Optional[List[str]] = None) -> str:

        ve = self._base + '/ve'

        ve_script = 'radical-utils-create-ve'
        ve_script_path = ru.which(ve_script)

        assert ve_script_path

        cmd = 'chmod 0700 %s' % ve_script_path
        cmd += ' && /bin/sh %s -v 3.8 -p %s' % (ve_script_path, ve)

        if modules:
            for module in modules:
                cmd += ' -m %s' % module

        out, err, ret = ru.sh_callout(cmd, shell=True)

        if ret != 0:
            raise RuntimeError('command failed: %s\nout: %s\nerr: %s'
                               % cmd, out, err)

        assert ret == 0

        for line in out.split('\n'):
            if line.startswith('VE_ACTIVATE: '):
                self._activate = '. ' + line.split(':', 1)[1].strip()
                break

        return ve

    def run(self, cmd: str, timeout: Optional[float] = None
            ) -> Tuple[int, str, str]:
        """
        run the given command over the connection, and return exit code, stdout
        and stderr.
        """

        if self._activate:
            _cmd = '%s; %s' % (self._activate, cmd)
        else:
            _cmd = cmd

        out, err, ret = ru.sh_callout(_cmd, shell=True)

        return ret, out, err

    def get_tunnel(self, rem_port: int, loc_port: Optional[int] = None) -> int:
        """
        Create a tunnel using the existing connection, from the remote port
        `rem_port` to the local port `loc_port`.  If the latter is not given,
        a random port is assigned.  Return the resulting local port.
        """
        if loc_port and loc_port != rem_port:
            raise ValueError('cannot reassign port for local tunnels')

        return rem_port


class SSHConnection(Connection):
    """A helper class to manage SSH endpoints.
    """

    def __init__(self, url: str,
                 config: JobExecutorConfig) -> None:
        """
        Initializes ssh helper.

        :param url: address at which to contact the remote sshd.
                    Supported schemas: `ssh://`
        :param config: AAA options
        """

        super().__init__(url, config)

        if self._url.schema not in ['ssh']:
            raise ValueError('expected `ssh://` as url schema')

        self._connect()

        if not self._base:
            self._base = self._rem_home + '/' + '.psij'

        cmd = 'mkdir -p %s' % self._base
        self._conn.run(cmd)

    def _connect(self) -> None:
        """
        Connect to the remote sshd endpoint.  If already connected, do nothing.
        """

        # FIXME: AAA
        self._conn = fabric.Connection(self._url.host)
        self._rem_home = self._conn.run('echo $HOME', hide=HIDE).stdout.strip()

    def bootstrap(self, rem_path: str = None,
                  modules: Optional[List[str]] = None) -> str:

        ve = self._base + '/ve'

        ve_script = 'radical-utils-create-ve'
        ve_script_path = ru.which(ve_script)

        assert ve_script_path

        # stage the required bootstrap scripts to remote
        # NOTE: this is not stable on concurrent client ops
        self._conn.put(ve_script_path, remote=self._base)

        cmd = 'chmod 0700 %s/%s' % (self._base, ve_script)
        cmd += ' && /bin/sh %s/%s -v 3.8 -p %s' % (self._base, ve_script, ve)

        if modules:
            for module in modules:
                cmd += ' -m %s' % module

        result = self._conn.run(cmd, hide=HIDE)

        for line in result.stdout.split('\n'):
            if line.startswith('VE_ACTIVATE: '):
                self._activate = '. ' + line.split(':', 1)[1].strip()
                break

        return ve

    def run(self, cmd: str, timeout: Optional[float] = None
            ) -> Tuple[int, str, str]:
        """
        run the given command over the connection, and return exit code, stdout
        and stderr.
        """

        if self._activate:
            _cmd = '%s; %s' % (self._activate, cmd)
        else:
            _cmd = cmd

        try:
            result = self._conn.run(_cmd, timeout=timeout, hide=HIDE)
            return result.exited, result.stdout, result.stderr

        except Exception as e:
            return -1, '', repr(e)


    def get_tunnel(self, rem_port: int, loc_port: Optional[int]) -> None:
        """
        Create a tunnel using the existing connection, from the remote port
        `rem_port` to the local port `loc_port`.  If the latter is not given,
        a random port is assigned.
        """
        raise NotImplementedError('tunnel is not yet implemented')
