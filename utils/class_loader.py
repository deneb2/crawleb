"""
Utility to dynamic loading classes
"""
import inspect

import spiders
from spiders import *

def spiders_loader(spider_list):
    """
    Load all the spider classes in the spiders directory
    except for base_spider.
    
    This function read all the files with "spider" in the name
    contained in the spiders directory.
    It loads all the classes that end with Spider. Or loads only
    the spiders specified in spider_list
    """
    if spider_list:
        spider_list = set(spider_list)
    result = []
    # checking all the files in spiders
    for names in dir(spiders):
        # picking up all the file with spider in the name
        # except for  base_spider
        if "spider" in names and names != "base_spider":
            module = getattr(spiders, names)
            if inspect.ismodule(module):
                for name, obj in inspect.getmembers(module):
                    # we do not want the base spider class
                    if (name != "BaseSpider" and name.endswith("Spider") and
                        (not spider_list or name in spider_list)):
                        if inspect.isclass(obj):
                            class_ = getattr(module, name)
                            instance = class_()
                            result.append(instance)
    return result
