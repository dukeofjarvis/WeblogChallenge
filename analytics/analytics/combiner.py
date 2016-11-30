from analytics.session import *

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