import urllib

def canonize(u):
    """
    Transform urls in a canonized form.

    We store canonized urls to better recognize the 
    same url in different forms. 
    INFO: This cannot be used for urls in refething/normal/priority lists
    Changing urls like this doesnt permit fetching.
    """
    u = u.rstrip("/")
    
    if u.startswith("https://"):
        u = u[8:]
    elif u.startswith("http://"):
        u = u[7:]
    try:
        u = urllib.quote(u)
    except:
        pass
        #todo: add logger
    return u

# Usefull constant used for rescheduling
HALF_HOUR = 1800
HOUR = HALF_HOUR * 2
TWO_HOURS = HOUR * 2
