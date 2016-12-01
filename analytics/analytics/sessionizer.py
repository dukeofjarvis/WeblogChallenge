from pyspark import SparkContext
from analytics.combiner import *
import json
import shutil
import os
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
        '''
        Utility function to convert sessions to a serializable
        JSON format

        :param sessions_rdd: RDD of sessions
        :return:
        '''
        return [session_serialize(s) for s in sessions_rdd.collect()]

    def calc_sessions(self, log_rdd, session_time_mins):
        '''
        Calculate the sessions from a RDD of log strings using
        a session time.

        Sessions are calculated by performing the following operations
        1. Map/Parse - Map each log entry to a IP key and a session
        created from the parsed entry
        2. Sort - The mapped sessions are sorted by start time (This
        operation could be removed if we can garauntee ordering of log
        entries)
        3. Combine - The sessions are combined together into sessions
        from the same IP if the time delta between sessions is less than
        or equal to the session time argument
        4. Map - The sessions are convereted to a flat map of session to
        remove the IP address key

        :param log_rdd: RDD of log entry string
        :param session_time_mins: session time to use for sessionization
        :return: An RDD of sessions
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
        '''
        Calculate the average session time in seconds from a list
        of sessions as a parallelized RDD

        This is calculated as follows
        1. Map - Map each session to a time delta by taking the
        difference between sessions end and start
        2. Reduce - Reduce the deltas by accumulating the deltas
        for all sessions
        2. Average - Divide the sum of deltas by the number of
        sessions

        :param sessions_rdd: list of sessions RDD
        :return: Average session time in seconds
        '''
        rdd1 = sessions_rdd.map(lambda x: x['end'] - x['start'])
        total = rdd1.reduce(lambda x, y : x + y)
        return float(total.total_seconds())/rdd1.count()

    def unique_visits_per_session(self, sessions_rdd):
        '''
        Calculate the unique URL visits per session from a list
        of sessions as a parallelized RDD

        This is calculated as follows

        1. Map - Map each session of a dict that includes the
        sessions id, ip and the number of unique requests
        2. Sort - Sort the mapped sessions by number of
        visits (Optional but displays better in Jupyter)
        3. Collect - Retrieve the list of visits per
        session

        :param sessions_rdd: list of sessions RDD
        :return: list of visits per session
        '''
        rdd1 = sessions_rdd.map(lambda x : dict(id=x['id'],ip=x['ip'],visits=len(set(x['requests']))))
        rdd2 = rdd1.sortBy(lambda x: -x['visits'])
        return rdd2.collect()

    def find_engaged_users(self, sessions_rdd):
        '''
        Calculate the unique URL visits per session from a list
        of sessions as a parallelized RDD

        This is calculated as follows

        1. Map - Map each session of a dict that includes the
        sessions ip and the duration of each session
        2. Sort - Sort the mapped sessions by number of length
         of the session
        3. Collect - Retrieve the list of session lengths

        :param sessions_rdd: list of sessions RDD
        :return: list of session lengths
        '''
        rdd1 = sessions_rdd.map(lambda x: dict(ip=x['ip'], length=(x['end'] - x['start']).total_seconds()))
        rdd2 = rdd1.sortBy(lambda x: -x['length'])
        return rdd2.collect()


def main(logfile, session_time_mins):
    '''
    Calculate the analytics of the web log
    from a logfile and a session time in minutes

    Displays result on terminal and saves JSON
    outputs

    :param logfile:
    :param session_time_mins:
    :return:
    '''

    outputdir = 'out'
    if os.path.exists(outputdir):
        shutil.rmtree(outputdir)
    os.makedirs(outputdir)

    ''' Create sessions from file '''
    sessionizer = Sessionizer(SparkContext(appName="Sessionizer"))
    sessions_rdd = sessionizer.calc_sessions_from_file(logfile, session_time_mins)
    sessions_path = os.path.join(outputdir,'sessions.json')
    with open(sessions_path, 'w') as file:
        json.dump(sessionizer.sessions_to_json(sessions_rdd), file)
    print "Sessions saved to " + sessions_path

    ''' Calculate average session time '''
    avg_time = sessionizer.average_session_time(sessions_rdd)
    print "Average session time " + str(avg_time)

    ''' Calculate unique visits per session '''
    unique_visits = sessionizer.unique_visits_per_session(sessions_rdd)
    visits_path = os.path.join(outputdir, 'visits.json')
    with open(visits_path, 'w') as file:
        json.dump(unique_visits, file)
    print "Unique visits per session saved to " + visits_path

    ''' Calculate longest sessions '''
    engaged = sessionizer.find_engaged_users(sessions_rdd)
    engage_path = os.path.join(outputdir, 'sessions_times.json')
    with open(engage_path, 'w') as file:
        json.dump(engaged, file)
    print "Longest sessions " + engage_path


if __name__ == "__main__":

    if len(sys.argv) < 2:
        sys.exit("Usage: logfile session_time_mins")
    main(sys.argv[1],int(sys.argv[2]))
