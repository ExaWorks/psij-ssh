"""A package containing :class:`psij.JobExecutor` implementations."""

from .ssh import SSHJobExecutor


__all__ = [
    'SSHJobExecutor'
]
