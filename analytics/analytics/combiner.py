from analytics.session import *

def combiner(s):
    '''
    Maps a session to the output object in
    a RDD combiner function.  The output object
    is a list of sessions

    Used by a RDD combiner function

    :param s: session
    :return: list containing the session
    '''
    slist = []
    slist.append(s)
    return slist

def make_merge_value_func(session_time_mins):
    '''
    Function to create a merge value function with
    a specific session time

    :param session_time_mins: session time in minutes
    :return: merge value fuction
    '''
    def merge_value(slist, s):
        """
        Merge a new session into the list of sessions
        for a specific IP

        Sessions are ordered in increasing order and this
        assumes that the new log time is greater or equal
        to all sessions in the list.  For this to hold
        this function should only be used on datasets
        that have been preordered by time

        Used by a RDD combiner function

        :param slist: session list
        :param s: new session
        :return: merged session list
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
    '''
    Function to create a merge combiner function with
    a specific session time

    :param session_time_mins: session time in minutes
    :return: merge combiner fuction
    '''
    def merge_combiners(slist1, slist2):
        '''
        Merge a session list into another list of sessions
        for a specific IP

        Sessions are ordered in increasing order and this
        assumes that the new log time is greater or equal
        to all sessions in the list.  For this to hold
        this function should only be used on datasets
        that have been preordered by time

        Used by a RDD combiner function

        :param slist1: session list
        :param slist2: session list
        :return: merged session list
        '''

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