"""Implement datatypes to describe documents"""

class Source():
    unknown = 0
    priority = 1
    normal = 2
    refetch = 3


class DocumentMetadata(object):
    def __init__(self, url=""):
        self.url = url
        
        self.depth = 0
        
        self.links = []
        
        self.alternatives = []
        
        self.delay = 0

        self.source = Source.unknown
        
        self.dhash = 0
        
        self.response = None

        self.status = None
