from pyspark import SparkContext
from analytics.session import *
import json
import sys
from datetime import *

def combiner(s):
    slist = []
    slist.append(s)
    return slist

def make_merge_value_func(session_time_mins):
    def merge_value(slist, s):
        """
        Merge a new log time into the list of sessions

        Sessions are ordered in increasing order and this
        assumes that the new log time is greater or equal
        to all sessions in the list.  For this to hold
        this function should only be used on datasets
        that have been preordered by time
        :param slist:
        :param s:
        :return:
        """
        last_session = slist[len(slist) - 1]
        if last_session['end'] > s['start']:
            raise SessionException("Dataset is not sorted by increasing time")
        if session_delta(last_session,s) < session_time_mins:
            slist[len(slist) - 1] = session_combine(last_session,s)
        else:
            slist.append(s)
        return slist
    return merge_value

def make_merge_combiners_func(session_time_mins):
    def merge_combiners(slist1, slist2):

        slist1_last_index = len(slist1) - 1
        last_session1 = slist1[slist1_last_index]
        if last_session1['end']  > slist2[0]['start']:
            raise SessionException("Dataset is not sorted by increasing time")
        if (session_delta(last_session1,slist2[0]) > session_time_mins):
            return slist1 + slist2
        else:
            cs = session_combine(last_session1,slist2[0])
            slist1[slist1_last_index] = cs
            return slist1 + slist2[1:]
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
        '''

        :param log_rdd:
        :param session_time_mins:
        :return:
        '''

        mapped_ips = log_rdd.map(lambda x:
                          (x.split()[2].split(":")[0],
                           session_from_entry(x)))

        sorted_ips = mapped_ips.sortBy(lambda x: x[1]['start'])
        session_time = timedelta(minutes=session_time_mins)
        self.sessions = sorted_ips.combineByKey(combiner, \
                             make_merge_value_func(session_time), \
                             make_merge_combiners_func(session_time)).collect()
        return [[session_serialize(x) for x in s[1]] for s in self.sessions]

    def average_session_time(self, sessions_rdd):

        rdd1 = sessions_rdd.map(lambda x : ('time',x[1].end - x[1].start))
        total = rdd1.reduce(lambda x, y : x + y)
        return total/rdd1.count()




def main(logfile, outputfile, session_time_mins):

    sessionizer = Sessionizer(SparkContext(appName="Sessionizer"))
    sessions = sessionizer.calc_sessions_from_file(logfile, session_time_mins)
    with open(outputfile, 'w') as file:
        json.dump(sessions, file)


if __name__ == "__main__":

    if len(sys.argv) < 4:
        sys.exit("Usage: logfile outputfile session_time_mins")
    main(sys.argv[1],sys.argv[2],int(sys.argv[3]))
