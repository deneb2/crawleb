'''Base strategy for refetching urls'''
import time

from core.metadata import Source
from schedulers.base_scheduler import BaseRefetchingStrategy
from utils.helpers import HALF_HOUR, HOUR, TWO_HOURS


class NewsRefetchingStrategy(BaseRefetchingStrategy):
    '''
    Strategy optimized for news
    News usually doesn't change by time. But they can be
    corrected after publishing.
    The idea is to refetch them after two hours for fresh news
    and waiting an exponential time after that. If the page doesn't change
    after 4 fetching is removed from the refetching list.
    '''
    def __init__(self, start_delay, refetching_delay):
        self.start_delay = start_delay
        self.refetching_delay = refetching_delay
        # this strategy refetch after two hours and after
        # exponentially 
        self.refetching_vector = [
            TWO_HOURS,
            refetching_delay,
            refetching_delay * 2,
            refetching_delay * 4,
        ]
        # we will remove None cases from refetching list
        self.increase_delay = {refetching_delay * 4: None}
        # on refetching we do not refetch more frequently than refetching_delay
        self.decrease_delay = {refetching_delay:refetching_delay}
        
        for i, v in enumerate(self.refetching_vector):
            if i < len(self.refetching_vector)-1:
                self.increase_delay[v] = self.refetching_vector[i+1]
            if i > 1:
                self.decrease_delay[v] = self.refetching_vector[i-1]

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
                # it means we do not want to periodically refetch
                # some specific urls
                doc.meta.souce = Source.refetch
                next_delay = self.refetching_delay

        elif is_new or not doc_meta.delay:
            # TODO: it is possible to do this only for new news
            #       but it requires to parse the page to check published_time
            # url is not a priority one AND
            # url is new or without previous delay -> init delay
            next_delay = self.refetching_vector[0]
            
        elif not is_changed or (doc_meta.response and
                                doc_meta.response.status_code != 200):
            # url is not changed or nort 200 status -> doubling the delay
            next_delay = self.increase_delay.get(doc_meta.delay, self.refetching_vector[1])
            
        else:
            next_delay = self.decrease_delay.get(doc_meta.delay, self.refetching_vector[2])

        assert(next_delay > 0 or next_delay == None)
        if not next_delay:
            return None, None

        expire = int(time.time()) + next_delay
        return expire, next_delay
    
