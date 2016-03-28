import netifaces as nif


def get_posixtime(uuid1):
    """Convert the uuid1 timestamp to a standard posix timestamp
    """
    assert uuid1.version == 1, ValueError('only applies to type 1')
    t = uuid1.time
    t -= 0x01b21dd213814000
    t /= 1e7
    return t


def mac_for_ip(ip):
    """Returns a list of MACs for interfaces that have given IP, returns None if not found"""
    for i in nif.interfaces():
        address = nif.ifaddresses(i)
        if_mac = if_ip = None
        if nif.AF_LINK in address:
            if len(address[nif.AF_LINK]) > 0:
                if 'addr' in address[nif.AF_LINK][0]:
                    if_mac = address[nif.AF_LINK][0]['addr']
        if nif.AF_INET in address:
            if len(address[nif.AF_INET]) > 0:
                if 'addr' in address[nif.AF_INET][0]:
                    if_ip = address[nif.AF_INET][0]['addr']
        if if_ip == ip:
            s = if_mac.replace(':', '')
            return long(s, 16)
    return None

print mac_for_ip('192.168.9.121')
