# Crawleb - a news crawler

[![Build Status](https://travis-ci.org/deneb2/crawleb.svg?branch=master)](https://travis-ci.org/deneb2/crawleb)

**Crawleb** is a crawler initially designed to crawl news. It can be used for any website.

## Why Crawleb
**Crawleb** makes easy to crawl the web since all you need is just a simple configuration.

This project started just for fun and it has been tested just for small crawling jobs.

Any advice, suggestion and collaboration is highly appreciated.

## Installation
**Crawleb** requires python2.7, redis and mongodb to work. Furtermore some python dependencies are required.

Installation provided is for Ubuntu Linux, but it should be easy to install it everywhere.

Installation assumes the use of *virtualenv*. It is anyway your choice to use it or not.

```
# sudo apt install redis mongodb virutalenv
# git clone https://github.com/deneb2/crawleb.git
# virtualenv env-crawleb
# source env-crawleb/bin/activate
# cd crawleb
# pip install -r requirements.txt
```

## Configuration
Inside the *conf* directory there is a configuration file called *conf.yaml*.
It is possible to configure here various settings:

 * refetching delay
 * mongodb and redis endpoints
 * output methods
 * logging files
 
The file is documented and easy to customise. Default setting should work for default system settings and a generic test environment.

To start crawling it is possible to start playing with *test_spider.py* or add a new class.

A new spider class should be in a file that contains the name *spider* and stored in the *spiders* directory.
The class will be loaded automatically if the name of the class ends with *Spider*.

For each spider is also possible to customize some few parameters.

This parameters are listed and documented inside the *base_spider.py* file.

Default configuration should be a good starting point.

## Start the crawler
To start the crawler enter you virtualenv end type:
```
# python tasks/crawleb.py
```
To stop the crawler it is possible to send a sigterm (_Ctrl-C_). The program will shut down gracefully (the time needed depends on your settings).

If you defined a different environment in the config file, and you want to use it, you can specify the name from the command line. Let say you environment is called _production_, you can start the crawler using:

```
# python tasks/crawleb.py -e production

```

If you do not want to start all the spiders, you can also specify a list of spider to use:

```
# python task/crawleb.py -s TestSpider AnotherSpider
```