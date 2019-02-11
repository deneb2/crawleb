#!/usr/bin/python

"""
This task starts the crawler using the configuration provided.

After loading the configuration, it set the logging queue and
the signal handler.
Afterwards loads all the spiders in the `spiders` directory.

The task will spawn one process for each spider and it will start
the crawling.
Each spider is compleatly independent.
"""
import signal
import logging
import argparse
import multiprocessing

from core.crawler import Crawler
from utils.class_loader import spiders_loader
from utils.config_reader import read_from_file
from utils.signals_handlers import GracefulKiller
from utils.logger_manager import log_listener, logger_configurer


def run_spider(cfg, s):
    crawl = Crawler(s, cfg['crawler'])
    crawl.start()


def main(args):
    config_file = "config/config.yml"

    environment = args.environment

    setting = read_from_file(config_file, environment)

    queue = multiprocessing.Queue(-1)

    # starting one process for logging
    listener = multiprocessing.Process(target=log_listener,
                                       args=(queue, setting['logging']['path']))
    listener.start()

    logger_configurer(queue)
    logger = logging.getLogger("main")
    logger.info("Crawleb starting...")

    # On ctrl-C we want a graceful exit.
    killer = GracefulKiller(queue)
    signal.signal(signal.SIGINT, killer.kill_signal_handler)

    logger.info("Configuration read!")

    spiders = spiders_loader(args.spiders)

    # starting one process for each spider
    processes = []

    for s in spiders:
        s.set_config()
        p = multiprocessing.Process(target=run_spider, args=(setting, s,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    queue.put_nowait(None)
    listener.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--environment", default="test",
                        help="select the configuration environment")
    parser.add_argument("-s", "--spiders", nargs='+', default=None,
                        help="specify a list of spiders to use. Default all")

    main(parser.parse_args())
