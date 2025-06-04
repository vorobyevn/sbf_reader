# -*- coding: utf-8 -*-
import time
from datetime import date
from datetime import datetime
from datetime import time as datetime_time
from dateutil.relativedelta import relativedelta
from datetime import timedelta

def now():
    return datetime.today()

def getDuration(startTime):
    p = now() - startTime
    return '{0}:{1}'.format(p.seconds//60, p.seconds%60)

def getDurationMinutes(startTime):
    p = now() - startTime
    return p.seconds//60

def getDurationHms(startTime):
    p = now() - startTime
    sec = p.seconds
    if sec < 60:
        return str(sec // 60)
    elif sec < 3600:
        return '{0}:{1}'.format(sec // 60, sec % 60)
    return str(timedelta(seconds=sec))

def getDateTime(dateStr, timeStr):
    t = parseTime(timeStr)
    d = parseDate(dateStr)
    return datetime.combine(d, t)

def parseTime(timeStr):
    return datetime.strptime(timeStr.strip(), '%H:%M').time()

def parseDate(dateTxt):
    return datetime.strptime(dateTxt, '%d.%m.%Y').date()

def parseDateTime(datetimeStr):
    return datetime.strptime(datetimeStr.strip(), "%d.%m.%Y %H:%M")

# return date or datetime
def calcDay(base_date, days_count):
    return base_date + timedelta(days=days_count)

# return datetime with 0 time
def calcDatetime(days_count, base_date = date.today()):
    return datetime.combine(base_date - relativedelta(days=days_count), datetime_time())

def timestamp(dt):
    return int(time.mktime(dt.timetuple()))

def fromTimestamp(ts):
    ts_i = ts
    if isinstance(ts, str) and ts.strip() != '':
        ts_i = int(ts)
    return datetime.fromtimestamp(ts_i)