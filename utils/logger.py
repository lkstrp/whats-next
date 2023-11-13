"""
Contains a custom logger class which uses some extra filters and handlers.
"""
import os
import datetime as dt
import logging

logging.addLevelName(logging.DEBUG, 'D')
logging.addLevelName(logging.INFO, 'I')

# noinspection PyUnresolvedReferences,PyAttributeOutsideInit
class FilterTimeTaker(logging.Filter):
    """Filter that takes the time since the last log message. This can be used as 'time_relative' in the log format."""

    def filter(self, record):
        """Overwrites the filter method to take the time since the last log message."""
        try:
            last = self.last
        except AttributeError:
            last = record.relativeCreated

        delta = dt.datetime.fromtimestamp(record.relativeCreated / 1000.0) - dt.datetime.fromtimestamp(
            last / 1000.0)

        duration_minutes = delta.seconds // 60  # Get the whole minutes
        duration_seconds = delta.seconds % 60  # Get the remaining seconds

        record.time_relative = '{:02d}:{:02d}'.format(duration_minutes, duration_seconds)
        self.last = record.relativeCreated

        return True

class Logger(logging.Logger):
    """
    TODO docstring not up to date
    """
    def __init__(self, name, path='logs.log'):
        self.name = name
        super().__init__(self.name)
        self.setLevel(logging.DEBUG)

        # Create formatters
        self._fmt_stream = logging.Formatter(
            fmt="[%(asctime)s %(time_relative)5s] %(levelname)s:%(lineno)d:%(funcName)s - %(message)s",
            datefmt="%H:%M:%S")
        self._fmt_file = logging.Formatter(
            fmt="[%(asctime)s %(time_relative)5s] %(levelname)s:%(lineno)d:%(funcName)s - %(message)s",
            datefmt="%y%m%d %H:%M:%S")

        # Create stream handler
        h_stream = logging.StreamHandler()
        h_stream.setLevel(logging.DEBUG)
        h_stream.setFormatter(self._fmt_stream)
        h_stream.addFilter(FilterTimeTaker())
        self.addHandler(h_stream)

        # Create file handler
        h_file = logging.FileHandler(path, delay=True)
        h_file.setLevel(logging.DEBUG)
        h_file.setFormatter(self._fmt_file)
        h_file.addFilter(FilterTimeTaker())
        self.addHandler(h_file)

        # Create pre_disabled_methods to allow disabling and enabling logging (see disable_logging and enable_logging)
        self._pre_disabled_methods = {}

    def change_log_file_path(self, new_log_file: str):
        """
        todo docstring not up to date
        Changes the log file path of the given logger. If the logger does not have a file handler, a new one
        is created.

        Args:
            logger (logging.Logger): Logger to change the log file path of.
            new_log_file (str): New log file path.
        """
        # Remove old file handler
        for handler in self.handlers:
            if isinstance(handler, logging.FileHandler):
                self.removeHandler(handler)
                break
        # If new_log_file is given, create a new file handler with it
        if new_log_file:
            # Create the directory if it does not exist
            os.makedirs(os.path.dirname(new_log_file), exist_ok=True)

            new_file_handler = logging.FileHandler(new_log_file)
            new_file_handler.setLevel(logging.DEBUG)
            new_file_handler.setFormatter(self._fmt_file)
            self.addHandler(new_file_handler)

    def change_log_level(self, new_log_level):
        """
        todo docstring not up to date
        Changes the log level of the given logger. If the logger does not have a level set, the new level will be applied.
        The new_log_level can be provided as an integer or a string representing the log level name.

        Args:
            logger (logging.Logger): Logger to change the log level of.
            new_log_level: New log level (int or str).
        """
        level_name_to_level = {
            'CRITICAL': logging.CRITICAL,
            'ERROR': logging.ERROR,
            'WARNING': logging.WARNING,
            'INFO': logging.INFO,
            'DEBUG': logging.DEBUG,
            'NOTSET': logging.NOTSET
        }

        if isinstance(new_log_level, str):
            new_log_level = new_log_level.upper()
            if new_log_level not in level_name_to_level:
                raise ValueError(f"Invalid log level name '{new_log_level}'.")
            new_log_level = level_name_to_level[new_log_level]

        if isinstance(new_log_level, int) and (0 <= new_log_level <= 50):
            self.setLevel(new_log_level)
        else:
            raise ValueError("Invalid log level. Log level must be an integer between 0 and 50 or a valid log"
                             " level name.")

    def disable_logging(self):
        self._pre_disabled_methods = {
            'debug': self.debug,
            'info': self.info,
            'warning': self.warning,
            'error': self.error,
            'critical': self.critical
        }

        self.debug = print
        self.info = print
        self.warning = print
        self.error = print
        self.critical = print

    def enable_logging(self):
        try:
            self.debug = self._pre_disabled_methods['debug']
            self.info = self._pre_disabled_methods['info']
            self.warning = self._pre_disabled_methods['warning']
            self.error = self._pre_disabled_methods['error']
            self.critical = self._pre_disabled_methods['critical']
        except KeyError:
            # Ignore since it was not disabled before
            pass
