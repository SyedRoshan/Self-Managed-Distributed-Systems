import logging, logging.handlers
import settings
from logging import StreamHandler, Formatter

event_logger = logging.getLogger()
event_logger.handlers = []
event_logger.setLevel(settings.LOG_LEVEL)
formatter = logging.Formatter(settings.LOG_FORMAT)
handler = logging.handlers.RotatingFileHandler(settings.LOG_FILENAME,
                                               maxBytes=settings.MAX_LOG_BYTES,
                                               backupCount=settings.MAX_OLD_LOGS,
)
handler.setFormatter(formatter)
event_logger.addHandler(handler)
logging.getLogger("pika").setLevel(logging.WARNING)

stats_logger = logging.getLogger('StatusLogger')
stats_logger.handlers = []
stats_logger.setLevel(settings.LOG_LEVEL)
formatter1 = logging.Formatter(settings.STATS_LOG_FORMAT)
handler1 = logging.handlers.RotatingFileHandler(settings.STAT_LOG_FILENAME,
                                               maxBytes=settings.MAX_LOG_BYTES,
                                               backupCount=settings.MAX_OLD_LOGS,
)
handler1.setFormatter(formatter1)
stats_logger.addHandler(handler1)
logging.getLogger("pika").setLevel(logging.WARNING)


TimeLog= logging.getLogger('TimeLogger')
TimeLog.handlers = []
TimeLog.setLevel(settings.TIME_LOG_LEVEL)
formatter2 = logging.Formatter(settings.TIME_LOG_FORMAT)
handler2 = logging.handlers.RotatingFileHandler(settings.Time_LOG_FILENAME,
                                               maxBytes=settings.MAX_LOG_BYTES,
                                               backupCount=settings.MAX_OLD_LOGS,
)
handler2.setFormatter(formatter2)
TimeLog.addHandler(handler2)
logging.getLogger("pika").setLevel(logging.WARNING)
