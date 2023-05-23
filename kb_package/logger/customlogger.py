# -*- coding: utf-8 -*-
"""
CustomLogger object. Use for logging information
"""

import logging
import os
import io
import sys
import traceback

from kb_package import tools


class CustomLogger:
    def __init__(
        self,
        name,
        log_dir=None,
        file_log_format="%(asctime)s %(name)-8s %(levelname)-8s %(message)s",
        **kwargs
    ):
        """
        Constructor of custom logger
        Examples:
            For max_size = 2 Ko==2 * 1024 bytes.
            >>> logger = CustomLogger("test", max_size=2 * 1024, \
                        base_file_name="test.txt")
            >>> logger.info("test")
        Args:
            name: str, the log name
            log_dir: log_dir: str|path, the path of the log if it going to
                using log_file
            file_log_format: str, the format of log display
            **kwargs: dict, it can includes params
                console: bool, default False
                console_log_format: str, default '%(name)-8s %(levelname)-8s
                    %(message)s'
                max_size: float, specify the max size that can reach the logger
                    file, default None (No limit)
                base_file_name: str, default os.path.join(log_dir, name,
                    ".txt"), use to specify the name style of log file.
                callback: fonction for callback after logging
                callback_each_logging: bool,
        """
        self.name = name
        self.log_dir = log_dir
        self.file_log_format = file_log_format
        self.console = kwargs.get("console", True)
        self.callback = kwargs.get("callback", None)
        self.callback_each_logging = kwargs.get("callback_each_logging", True)
        self._last_end = "\n"
        if callable(self.callback):
            self.log_capture_string = io.StringIO()
        else:
            self.log_capture_string = None
            self.callback = None

        self.console_log_format = kwargs.get(
            "console_log_format", "%(name)-8s %(levelname)-8s %(message)s"
        )
        self.max_size = kwargs.get("max_size", None)
        self.base_file_name = kwargs.get(
            "base_file_name",
            None
            if self.log_dir is None
            else os.path.join(self.log_dir, str(name) + ".txt"),
        )
        self.last_file_name = None
        self.writer = None

        if self.log_dir is not None:
            self.base_file_name = os.path.join(
                self.log_dir, os.path.basename(self.base_file_name)
            )
        else:
            if self.base_file_name is not None:
                self.log_dir = os.path.dirname(self.base_file_name)

        if self.base_file_name is not None:
            self.base_file_name, self.extension = os.path.splitext(
                self.base_file_name
            )

        self._create_new_logger_handler()

    def send_all_logger_message_by_callback(self):
        if isinstance(self.log_capture_string, io.StringIO):
            self.callback(self.log_capture_string.getvalue())
            self.log_capture_string.truncate(0)
            self.log_capture_string.seek(0)

    @staticmethod
    def get_logger(name=None):
        """
        use to got new logger object
        Args:
            name: str

        Returns:
            logging.getLoggerClass(), logger object

        """
        if name is not None:
            return logging.getLogger(name)
        return logging.getLogger()

    def _create_new_logger_handler(self):
        """
        use to instantiate the logger use the config
        Returns:

        """

        # Create logger
        logger = self.get_logger(self.name)
        logger.setLevel(logging.INFO)
        file_handler_formatter = logging.Formatter(self.file_log_format,
                                                   "%Y-%m-%d %H:%M:%S")
        if self.log_capture_string:
            ch = logging.StreamHandler(self.log_capture_string)
            ch.setFormatter(file_handler_formatter)
            logger.addHandler(ch)
        # Set FileHandler
        if self.base_file_name is not None:
            log_file = self.base_file_name
            if self.max_size is not None:
                log_file += "_" + (tools.CustomDateTime.datetime_as_string(
                    sep="-", microsecond=True
                )).replace(":", "_").replace(" ", "__")
            log_file += self.extension
            if not os.path.isdir(self.log_dir):
                os.makedirs(self.log_dir, exist_ok=True)

            handler = logging.FileHandler(log_file)
            handler.setFormatter(file_handler_formatter)
            logger.addHandler(handler)
            self.last_file_name = log_file

        if self.console:
            console_handler_formatter = logging.Formatter(
                self.console_log_format
            )
            console_handler = logging.StreamHandler(
                sys.stderr
            )  # sys.stderr or sys.stdout
            console_handler.setFormatter(console_handler_formatter)
            logger.addHandler(console_handler)

        self.writer = logger

    @property
    def log_file(self):
        if self.base_file_name is None:
            return None
        try:
            self.writer.handlers[0].stream.truncate()
        except OSError:
            self._create_new_logger_handler()
        return self.writer.handlers[0].stream

    def _log(self, msg, *args, level="INFO", **kwargs):
        """
        Use to log
        Args:
            msg: str, the msg that you want to log
            *args: list|tuple, args for msg formatting
            level: str, set of (INFO|WARNING|EXCEPTION|CRITICAL|ERROR)

        Returns:

        """
        self._last_end = kwargs.pop("end", "\n")
        recreate = False
        try:
            self.writer.handlers[0].stream.truncate()
        except OSError:
            recreate = True

        if self.last_file_name is not None:
            if self.max_size is not None or recreate:
                if recreate or os.stat(self.last_file_name).st_size > self.max_size:
                    try:
                        while self.writer.hasHandlers():
                            handler = self.writer.handlers[0]
                            handler.flush()
                            handler.close()
                            self.writer.removeHandler(handler)
                    except:
                        print(traceback.format_exc())
                    try:
                        base = os.path.splitext(
                            os.path.basename(self.last_file_name)
                        )[0]
                        tools.rename_file(
                            path_to_last_file=self.last_file_name,
                            new_name=base
                            + "_to_"
                            + tools.CustomDateTime.datetime_as_string(
                                sep="-", microsecond=True
                            )
                            + self.extension,
                        )
                    except:
                        print(traceback.format_exc())

                    self._create_new_logger_handler()
        try:
            msg = str(msg) % args
        except (TypeError, Exception):
            msg = " ".join([str(msg)] + [str(p) for p in args])
        getattr(self.writer, level.lower())(msg, **kwargs)
        if self.callback and self.callback_each_logging:
            self.send_all_logger_message_by_callback()

    def __call__(self, *args, **kwargs):
        self._log(*args, **kwargs)

    def info(self, msg="\n", *args, **kwargs):
        """
        log info
        Args:
            msg: str, the msg that you want to log
            *args: list|tuple, args for msg formatting

        Returns:

        """
        self._log(msg, *args, level="INFO", **kwargs)

    def exception(self, msg="\n", *args, **kwargs):
        """
        log exception
        Args:
            msg: str, the msg that you want to log
            *args: list|tuple, args for msg formatting

        Returns:

        """
        self._log(msg, *args, level="EXCEPTION", **kwargs)

    def critical(self, msg="\n", *args, **kwargs):
        """
        log critical
        Args:
            msg: str, the msg that you want to log
            *args: list|tuple, args for msg formatting

        Returns:

        """
        self._log(msg, *args, level="CRITICAL", **kwargs)

    def error(self, msg="\n", *args, **kwargs):
        """
        log error
        Args:
            msg: str, the msg that you want to log
            *args: list|tuple, args for msg formatting

        Returns:

        """
        self._log(msg, *args, level="ERROR", **kwargs)

    def warning(self, msg="\n", *args, **kwargs):
        """
        log warning
        Args:
            msg: str, the msg that you want to log
            *args: list|tuple, args for msg formatting

        Returns:

        """
        self._log(msg, *args, level="WARNING", **kwargs)


if __name__ == '__main__':
    logger = CustomLogger("Worker", callback=print)
    logger.info("OK")
    logger.info("Non %s test %s", "ok")
