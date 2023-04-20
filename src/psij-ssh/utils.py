
import threading
from typing import Optional, List, Dict, Any

from psij import (JobExecutorConfig)

import radical.utils as ru


class SSHUtils(object):
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
        ru_url = ru.Url(url)
        if ru_url.schema not in ['ssh']:
            raise ValueError('expected `ssh://` as url schema')


    def connect(self) -> None:
        """
        Connect to the remote sshd endpoint.  If already connected, do nothing.
        """
        pass

    def get_tunnel(self, rem_port: int, loc_port: Optional[int]) -> None:
        """
        Create a tunnel using the existing connection, from the remote port
        `rem_port` to the local port `loc_port`.  If the latter is not given,
        a random port is assigned.
        """
        connect()
        pass
