import logging
import logging.handlers
import os
import time
import socket
import logaugment

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')


# https://inneka.com/programming/python/python-logging-use-milliseconds-in-time-format/
class MyFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            if "%F" in datefmt:
                msec = "%03d" % record.msecs
                datefmt = datefmt.replace("%F", msec)
            s = time.strftime(datefmt, ct)
        else:
            t = time.strftime("%Y-%m-%d %H:%M:%S", ct)
            s = "%s,%03d" % (t, record.msecs)
        return s


def setup_logger(application):
    """

    :return:
    """
    log_level = logging.getLevelName(LOG_LEVEL)

    logger = logging.getLogger('CUSTOM')
    logger.setLevel(log_level)
    logaugment.set(logger, metadata="")

    host_name = socket.gethostname()

    formatter_syslog = MyFormatter(
        fmt=host_name + " %(asctime)s[%(levelname)s]["+application+"]: [MSG]%(message)s %(metadata)s",
        datefmt="%Y-%m-%d %H:%M:%S,%F%z"
    )

    formatter_syslog.converter = time.gmtime

    formatter_stream = MyFormatter(
        fmt="%(asctime)s[%(levelname)s]["+application+"]: [MSG]%(message)s %(metadata)s",
        datefmt="%Y-%m-%d %H:%M:%S,%F%z"
    )

    formatter_stream.converter = time.gmtime

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter_stream)
    logger.addHandler(stream_handler)

    RSYSLOG_HOST = os.getenv('RSYSLOG_HOST')
    RSYSLOG_PORT = os.getenv('RSYSLOG_PORT')

    if RSYSLOG_HOST is None and RSYSLOG_PORT is None:
        logger.warning("RSYSLOG is DISABLED. Set RSYSLOG_HOST and RSYSLOG_PORT environment variables to ENABLE")
    else:
        syslog_handler = logging.handlers.SysLogHandler(
            address=(RSYSLOG_HOST, int(RSYSLOG_PORT))
        )
        syslog_handler.setFormatter(formatter_syslog)
        logger.addHandler(syslog_handler)

    return logger


def set_metadata(logger, metadata_json):
    metadata = "[METADATA]{}".format(metadata_json)
    logaugment.set(logger, metadata=metadata)
