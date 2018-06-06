"""Signals Handlers."""
class GracefulKiller(object):
    """Implements a way to gracefull exit from crawling."""
    kill_now = False

    def __init__(self, q):
        self.queue = q
    
    def kill_signal_handler(self, signum, frame):
        GracefulKiller.kill_now = True
        # avoid listener hanging up
        # self.queue.put(None)
