"""Logging utility"""
import logging
import logging.handlers

class QueueHandler(logging.Handler):
    """
    Store all the log in a queue.

    Since we have multiple process we organize the logs
    in a queue. A process will store the queue afterwards.
    """
    def __init__(self, queue):
        logging.Handler.__init__(self)
        self.queue = queue

    def emit(self, record):
        try:
            ei = record.exc_info
            if ei:
                dummy = self.format(record)
                record.exc_info = None
            self.queue.put_nowait(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

def log_listener(queue, filename):
    """
    Setting up the logger.

    This function also read from the queue and write on the specified file the log.
    """
    root = logging.getLogger()
    # TODO: configurtion can be parametrized
    h = logging.handlers.RotatingFileHandler(filename, 'a', 30000000, 10)
    f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
    h.setFormatter(f)
    root.addHandler(h)

    while True:
        try:
            record = queue.get()
            if record is None:
                print "None detected. Program is shutting down."
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except (KeyboardInterrupt, SystemExit):
            #raise
            logger.info("Exit signal detected - shutting down.")
            break
        except:
            import sys, traceback
            print >> sys.stderr, 'Whoops! Problem:'
            traceback.print_exc(file=sys.stderr)

def logger_configurer(queue):
    """Helper function for setting up the logger."""
    h = QueueHandler(queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(logging.DEBUG)
