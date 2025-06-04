# -*- coding: utf-8 -*-
import logging
import re
import sys
from ad_types import adTypes
import urllib
import urllib.parse as urlparse

re_digit = re.compile(r'\D')

def print_err(*args):
    sys.stderr.write(' '.join(map(str,args)) + '\n')

def printInfo(str, printStr = True):
    logging.info(str)
    if printStr:
        try:
            print(str)
        except Exception as ex:
            logging.info("Print info error")

def printError(str, printStr = True):
    logging.error(str)
    if printStr:
        try:
            print_err(str)
        except Exception as ex:
            logging.info("Print error")

def printDebug(str, printStr = True):
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.debug(str)
        if printStr:
            try:
                print(str)
            except Exception as ex:
                logging.info("Print error")

def screenshot(driver, file):
    driver.save_screenshot("logs\{0}".format(file))

def get_number(src):
    return re_digit.sub('', src)

# Sum db save results
def summ(total, saved):
    ins, upd = tuple(sum(t) for t in zip(total, saved))
    return ins, upd

def getValue(obj, field, default=''):
    if field in obj and obj[field] is not None:
        return obj[field]
    else:
        return default

def getValueFromString(src_str, regex):
    s = re.search(regex, src_str)
    if s is None or s.lastindex == 0:
        return None
    return s.groups()[0]

def addItem(array, item):
    if item is None:
        return
    item = str(item)
    if item.strip() != '':
        array.append(item)

def add_base_url(base, url):
    # if url.find(settings.base_url) < 0:
    if len(url) > 0 and url[0] == "/":
        url = base + url
    return url

def url2params(base_url):
    parsed = urlparse.urlparse(base_url)
    params = urlparse.parse_qs(parsed.query, keep_blank_values=True)
    for par in params:
        params[par] = params[par][0]
    return params

def get_url(base_url, values):
    parsed = urlparse.urlparse(base_url)
    params = urlparse.parse_qs(parsed.query, keep_blank_values=True)
    for val in values:
        params[val] = [values[val]]
    q = urlparse.unquote(urlparse.urlencode(params, doseq=True))
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, q, parsed.fragment))

def getMetroDistance(dist_type_foot, dist_min):
    if dist_type_foot:
        return dist_min
    else:
        return int(dist_min) * 100

def splitArray(seq, size):
    return [seq[i:i + size] for i in range(0, len(seq), size)]