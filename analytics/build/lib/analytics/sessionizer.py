from pyspark import SparkContext
from analytics.session import SessionException
from analytics.session import Session
import iso8601
import json
import sys
from datetime import *

def combiner(x):
    c = []
    c.append(Session(x, x))
    return c

def make_merge_value_func(session_time_mins):
    def merge_value(s1, x):
        """
        Merge a new log time into the list of sessions

        Sessions are ordered in increasing order and this
        assumes that the new log time is greater or equal
        to all sessions in the list.  For this to hold
        this function should only be used on datasets
        that have been preordered by time
        :param s1:
        :param x:
        :return:
        """
        s2 = Session(x, x)
        last = len(s1) - 1
        l = s1[last]
        if l.end > s2.start:
            raise SessionException("Dataset is not sorted by increasing time")
        if s2.delta(l) < session_time_mins:
            s1[last] = l.combine(s2)
        else:
            s1.append(s2)
        return s1
    return merge_value

def make_merge_combiners_func(session_time_mins):
    def merge_combiners(s1, s2):
        last1 = len(s1) - 1
        if s1[last1].end > s2[0].start:
            raise SessionException("Dataset is not sorted by increasing time")
        if (s1[last1].delta(s2[0]) > session_time_mins):
            return s1 + s2
        else:
            cs = s1[last1].combine(s2[0])
            s1[last1] = cs
            return s1 + s2[1:]
    return merge_combiners

class Sessionizer(object):

    def __init__(self, sc):
        self.sc = sc

    def calc_sessions_from_file(self, log_file, session_time_mins):
        log_rdd = self.sc.textFile(log_file)
        return self._calc_sessions(log_rdd, session_time_mins)

    def calc_sessions_from_list(self, log_list, session_time_mins):
        log_rdd = self.sc.parallelize(log_list)
        return self._calc_sessions(log_rdd, session_time_mins)

    def _calc_sessions(self, log_rdd, session_time_mins):

        mapped_ips = log_rdd.map(lambda x:
                          (x.split()[2].split(":")[0],
                           iso8601.parse_date(x.split()[0])))

        sorted_ips = mapped_ips.sortBy(lambda x: x[1])
        session_time = timedelta(minutes=session_time_mins)
        sessions = sorted_ips.combineByKey(combiner, \
                             make_merge_value_func(session_time), \
                             make_merge_combiners_func(session_time)).collect()
        return [(s[0],[x.serialize() for x in s[1]]) for s in sessions]

def main(logfile, outputfile, session_time_mins):

    sessionizer = Sessionizer(SparkContext(appName="Sessionizer"))
    sessions = sessionizer.calc_sessions_from_file(logfile, session_time_mins)
    with open(outputfile, 'w') as file:
        json.dump(sessions, file)


if __name__ == "__main__":

    if len(sys.argv) < 4:
        sys.exit("Usage: logfile outputfile session_time_mins")
    main(sys.argv[1],sys.argv[2],int(sys.argv[3]))
