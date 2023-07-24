"""The package containing the jobs module of this PSI implementation."""

import os as _os
__version__ = open('%s/VERSION' % _os.path.dirname(__file__)).read().strip()

from .utils import SSHConnection, LocalConnection
