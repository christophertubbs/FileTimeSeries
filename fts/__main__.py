#!/usr/bin/env python
"""
Watches a directory and its subdirectories for operations on files and records them to disk
"""
import typing
import logging
import argparse
import pathlib
import sys

if __name__ == "__main__":
    from utilities.logging import setup_logging
    setup_logging()

LOGGER: typing.Final[logging.Logger] = logging.getLogger(pathlib.Path(__file__).stem)


class Arguments:
    def __init__(
        self,
        *args: str,
        directory: pathlib.Path = None,
        polling_interval: float = None,
        output: pathlib.Path = None,
    ) -> None:
        self.directory: pathlib.Path = directory
        self.polling_interval: float = polling_interval or 1.0
        self.output: pathlib.Path = output
        self.__parse(args)
        self.__validate()

    def __validate(self):
        """
        Throw exceptions if the arguments aren't valid
        """
        if not self.directory.is_dir():
            raise ValueError(f"{self.directory} is not a valid directory")

        if self.output.is_dir():
            raise ValueError(f"{self.output} must be a file, not a directory")

    def __parse(self, args: typing.Sequence[str]):
        """
        Parse the CLI inputs
        """

        parser: argparse.ArgumentParser = argparse.ArgumentParser(
            "Watches a directory and its subdirectories and writes information about file use to a database"
        )

        if isinstance(self.directory, pathlib.Path) and self.directory.exists():
            parser.add_argument(
                "--directory",
                dest="directory",
                type=pathlib.Path,
                default=self.directory,
                help="The directory to watch",
            )
        else:
            parser.add_argument(
                "directory",
                type=pathlib.Path,
                help="The directory to watch",
            )

        parser.add_argument(
            "--polling-interval",
            "-i",
            dest="polling_interval",
            default=self.polling_interval,
            type=float,
            help="The frequency in seconds to wait between polls",
        )

        if isinstance(self.output, pathlib.Path) and self.output.exists():
            parser.add_argument(
                "--output",
                dest="output",
                type=pathlib.Path,
                default=self.output,
                help="The output file to write to",
            )
        else:
            parser.add_argument(
                "output",
                type=pathlib.Path,
                help="The output file to write to",
            )

        parameters: argparse.Namespace = parser.parse_args(args) if args else parser.parse_args()

        self.directory = parameters.directory
        self.polling_interval = parameters.polling_interval
        self.output = parameters.output

def main() -> int:
    """
    The main entry point for the file watcher
    :return: The exit code
    """
    arguments: Arguments = Arguments()
    try:
        # Call the application logic
        pass
    except Exception as exception:
        LOGGER.error(f"Could not monitor {arguments.directory}: {exception}", exc_info=exception, stack_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())