import os
import logging, logging.handlers
import time
import datetime
from multiprocessing import Lock

SuccessMessageCount = 0
FailureCount = 0
EmptyMessageCount = 0
HourStartTime = None
TotalTimeSpan = None
ElapsedActiveTime = 0
IntermediateStartTime = None
AverageParsingTimePerDoc = None


class Counter(object):
    def __init__(self):
        self.lock = Lock()
        global SuccessMessageCount
        global FailureCount
        global EmptyMessageCount
        global HourStartTime
        global TotalTimeSpan
        global ElapsedActiveTime
        global IntermediateStartTime
        global AverageParsingTimePerDoc

    def reset_counters(self):
        logging.info('Reset counter')
        global SuccessMessageCount
        global FailureCount
        global EmptyMessageCount
        global HourStartTime
        global TotalTimeSpan
        global ElapsedActiveTime
        global IntermediateStartTime
        global AverageParsingTimePerDoc

        SuccessMessageCount = 0
        FailureCount = 0
        EmptyMessageCount = 0
        HourStartTime = time.time()
        TotalTimeSpan = time.time()
        ElapsedActiveTime = None
        IntermediateStartTime = None
        AverageParsingTimePerDoc = None

    def increment_success(self):
        global SuccessMessageCount
        with self.lock:
            SuccessMessageCount += 1

    def increment_failure(self):
        global FailureCount
        with self.lock:
            FailureCount += 1

    def increment_empty(self):
        global EmptyMessageCount
        with self.lock:
            EmptyMessageCount += 1

    def pause_stopwatch(self):
        global IntermediateStartTime
        global ElapsedActiveTime
        if IntermediateStartTime is not None:
            with self.lock:
                if ElapsedActiveTime is None:
                    ElapsedActiveTime = 0
                if IntermediateStartTime is None:
                    return
                ElapsedActiveTime += time.time() - IntermediateStartTime
                IntermediateStartTime = None

    def start_stopwatch(self):
        global IntermediateStartTime
        if IntermediateStartTime is None:
            with self.lock:
                IntermediateStartTime = time.time()

    def get_snapshot_restart(self):
        response = None
        self.pause_stopwatch()
        with self.lock:
            response = self.get_time_log()
            self.reset_counters()
        return response

    def convert_datetime(self, epoch_time):
        return datetime.datetime.fromtimestamp(epoch_time).strftime('%Y-%m-%d %H:%M:%S.%f')

    def get_timespan(self, startTime, endTime):
        return self.convert_timespan(endTime - startTime)

    def convert_timespan(self, epochTime):
        if epochTime <= 0:
            return ("00:00:00.000")

        # Estimate hours and minuets
        hours, rem = divmod(epochTime, 3600)
        minutes, seconds = divmod(rem, 60)
        return ("{:0>2}:{:0>2}:{:05.3f}".format(int(hours), int(minutes), seconds))

    def get_time_log(self):
        response = None
        #self.pause_stopwatch()
        totalmessagecount = SuccessMessageCount + FailureCount + EmptyMessageCount

        response = 'PostingStatus' + '|' + str(self.convert_datetime(HourStartTime)) + '|' + str(self.convert_datetime(time.time()))
        if ElapsedActiveTime is not None:
            response += '|' + str(self.convert_timespan(ElapsedActiveTime)) + '|'+ str(self.get_timespan(ElapsedActiveTime, (time.time() - TotalTimeSpan)))
        else:
            response += '|' + str('00:00:00.000000') + '|' + str(self.get_timespan(HourStartTime, (time.time() - TotalTimeSpan)))
        response += '|' + str(totalmessagecount) + '|' + str(SuccessMessageCount) + '|' + str(FailureCount) + '|' + str(
            EmptyMessageCount)
        return response
