"""Implements datatypes to describe the document content"""
class Document(object):
   """Document data."""
   def __init__(self, entries = {}):
      
      self.url = entries.get("url", "")
      
      self.raw_html = entries.get("raw_html", "")
      
      self.blocked = entries.get("blocked", False)
      
      self.fetched_time = entries.get("fetched_time", "")
      
      self.status = entries.get("status", 0)
      
      self.history = entries.get("history", [])
      
      self.dhash = entries.get("hash", 0)
      
      self.domain = entries.get("domain", "")
      
      self.comments = entries.get("comments", 0)
      
      self.social = entries.get("social", {})
      
   @property
   def info(self):
      """Return a dict with all the information."""
      data = {
         "url": self.url,
         "raw_html": self.raw_html,
         "blocked": self.blocked,
         "fetched_time": self.fetched_time,
         "status": self.status,
         "history": self.history,
         "hash": self.dhash,
         "domain": self.domain,
         "comments": self.comments,
         "social": self.social,
      }
      return data
   
   @property
   def short_info(self):
      """
      Return a dict with some information.
      This function can be usefull when we want to share some information without
      moving a lot of data (like the raw_html). Or for debugging purpose.
      """
      data = {
         "url": self.url,
         "fetched_time": self.fetched_time,
         "status": self.status,
         "domain": self.domain,
         "comments": self.comments,
         "social": self.social,
      }
      return data
