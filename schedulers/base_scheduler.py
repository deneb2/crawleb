'''Base strategy for refetching urls'''
import time

from core.metadata import Source

# Usefull constant used for rescheduling
HALF_HOUR = 1800
HOUR = HALF_HOUR * 2


class BaseRefetchingStrategy():
    '''
    Basic strategy to compute the date to refectch next
    mechanism:
      - priority url are refetched any start_delay seconds
      - the other urls are refetched after refetching_delay
        if the content changes the delay will be halved
        if the content doesn't change delay will be doubled
    '''
    def __init__(self, start_delay, refetching_delay):
        self.start_delay = start_delay
        self.refetching_delay = refetching_delay
        
    def compute(self, doc_meta, is_new, is_changed):
        """
        Heuristic to determine next refetch

        Return a couple:
        expire     -- the next date we want to refetch(epoch)
        next_delay -- the current delay used
        """
        next_delay = 0
        if doc_meta.source == Source.priority:
            if self.start_delay:
                next_delay = self.start_delay
            else:
                # start_delay is not set. removing from priority
                # it means we do not want to periodically refatch
                # some specific urls
                doc_meta.souce = Source.refetch
                next_delay = self.refetching_delay

        elif is_new or not doc_meta.delay:
            # url is not a priority one AND
            # url is new or without previous delay -> init delay
            next_delay = self.refetching_delay
            
        elif not is_changed or (doc_meta.response and
                                doc_meta.response.status_code != 200):
            # url is not changed -> doubling the delay
            # status is not 200  -> doubling the delay
            next_delay = doc_meta.delay * 2
            
        else:
            # url is changed -> halving the delay (but not less than 1 HOUR)
            if doc_meta.delay > HOUR:
                next_delay = doc_meta.delay / 2
            else:
                next_delay = doc_meta.delay

        assert(next_delay > 0)
        expire = int(time.time()) + next_delay
        return expire, next_delay
    
