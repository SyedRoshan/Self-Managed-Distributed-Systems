import threading
from counter import Counter
from Logger import stats_logger


class TaskThread(threading.Thread):
    """
    Thread that executes a task every N seconds
    """
    stats = Counter()

    def __init__(self, interval):
        threading.Thread.__init__(self)
        self._finished = threading.Event()
        self._interval = interval
    
    def setInterval(self, interval):
        """
        Set the number of seconds we sleep between executing our task
        """
        self._interval = interval
    
    def shutdown(self):
        """
        Stop this thread
        """
        self._finished.set()
    
    def run(self):
        while 1:
            if self._finished.isSet(): return
            self.do_task()
            
            # sleep for interval or until shutdown
            self._finished.wait(self._interval)
    
    def do_task(self):
        """
        Log the data processing statistics
        """
        logData = self.stats.get_snapshot_restart()

        if logData is None:
            logData = ''

        stats_logger.info(logData)


