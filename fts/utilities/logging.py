"""
Common logging setup
"""
import typing
import logging

_MESSAGE_FORMAT: typing.Final[str] = "[%(asctime)s] %(levelname)s %(filename)s:%(lineno)s %(message)s"
"""The format for log messages"""

_DATE_FMT: typing.Final[str] = "%Y-%m-%d %H:%M:%S%z"
"""The format for date and time"""

def setup_logging(log_level: int = logging.INFO) -> None:
    """
    A common function to set up logging regardless of entrypoint
    """
    logging.basicConfig(
        level=log_level,
        format=_MESSAGE_FORMAT,
        datefmt=_DATE_FMT,
    )