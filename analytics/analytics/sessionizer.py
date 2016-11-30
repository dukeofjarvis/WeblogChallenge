from pyspark import SparkContext
from analytics.combiner import *
import json
import sys
from datetime import *


class Sessionizer(object):

    def __init__(self, sc):
        self.sc = sc

    def calc_sessions_from_file(self, log_file, session_time_mins):
        log_rdd = self.sc.textFile(log_file)
        return self.calc_sessions(log_rdd, session_time_mins)

    def calc_sessions_from_list(self, log_list, session_time_mins):
        log_rdd = self.sc.parallelize(log_list)
        return self.calc_sessions(log_rdd, session_time_mins)

    def sessions_to_json(self, sessions_rdd):
        return [session_serialize(s) for s in sessions_rdd.collect()]

    def calc_sessions(self, log_rdd, session_time_mins):
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
        combined_sessions = sorted_ips.combineByKey(combiner, \
                             make_merge_value_func(session_time), \
                             make_merge_combiners_func(session_time))
        sessions_rdd = combined_sessions.flatMap(lambda x: x[1])
        return sessions_rdd

    def average_session_time(self, sessions_rdd):
        rdd1 = sessions_rdd.map(lambda x: x['end'] - x['start'])
        total = rdd1.reduce(lambda x, y : x + y)
        return int(float(total.total_seconds())/float(rdd1.count()))

    def unique_visits_per_session(self, sessions_rdd):
        rdd1 = sessions_rdd.map(lambda x : (x['id'],dict(ip=x['ip'],visits=len(set(x['requests'])))))
        return rdd1.collectAsMap()

    def find_engaged_users(self, sessions_rdd):
        rdd1 = sessions_rdd.map(lambda x: dict(ip=x['ip'], length=(x['end'] - x['start']).total_seconds()))
        rdd2 = rdd1.sortBy(lambda x: -x['length'])
        return rdd2.collect()


def main(logfile, outputfile, session_time_mins):

    sessionizer = Sessionizer(SparkContext(appName="Sessionizer"))
    sessions_rdd = sessionizer.calc_sessions_from_file(logfile, session_time_mins)
    with open(outputfile, 'w') as file:
        json.dump(sessionizer.sessions_to_json(sessions_rdd), file)


if __name__ == "__main__":

    if len(sys.argv) < 4:
        sys.exit("Usage: logfile outputfile session_time_mins")
    main(sys.argv[1],sys.argv[2],int(sys.argv[3]))
