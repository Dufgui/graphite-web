"""Copyright 2008 Orbitz WorldWide

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

from graphite.logger import log
from graphite.storage import STORE
from graphite.readers import FetchInProgress
from django.conf import settings
from graphite.util import epoch

class TimeSeries(list):
  def __init__(self, name, start, end, step, values, consolidate='average'):
    list.__init__(self, values)
    self.name = name
    self.start = start
    self.end = end
    self.step = step
    self.consolidationFunc = consolidate
    self.valuesPerPoint = 1
    self.options = {}


  def __iter__(self):
    if self.valuesPerPoint > 1:
      return self.__consolidatingGenerator( list.__iter__(self) )
    else:
      if(isinstance(self.step, list) and len(self.step) < len(self)):
        return list.__iter__(self[:len(self.step)])
      else:  
        return list.__iter__(self)


  def consolidate(self, valuesPerPoint):
    self.valuesPerPoint = int(valuesPerPoint)


  def __consolidatingGenerator(self, gen):
    buf = []
    for x in gen:
      buf.append(x)
      if len(buf) == self.valuesPerPoint:
        while None in buf: buf.remove(None)
        if buf:
          yield self.__consolidate(buf)
          buf = []
        else:
          yield None
    while None in buf: buf.remove(None)
    if buf: yield self.__consolidate(buf)
    else: yield None
    raise StopIteration


  def __consolidate(self, values):
    usable = [v for v in values if v is not None]
    if not usable: return None
    if self.consolidationFunc == 'sum':
      return sum(usable)
    if self.consolidationFunc == 'average':
      return float(sum(usable)) / len(usable)
    if self.consolidationFunc == 'max':
      return max(usable)
    if self.consolidationFunc == 'min':
      return min(usable)
    raise Exception("Invalid consolidation function!")


  def __repr__(self):
    return 'TimeSeries(name=%s, start=%s, end=%s, step=%s, values=%s)' % (self.name, self.start, self.end, self.step, list.__repr__(self))

  def __eq__(self, other):
    if isinstance(other, self.__class__):
      return self.name==other.name and self.start==other.start and self.end==other.end and self.step==other.step
    else:
      return False
    #return isinstance(other, TimeSeries) and other.equalityprop == self.equalityprop
    

  def getInfo(self):
    """Pickle-friendly representation of the series"""
    return {
      'name' : self.name,
      'start' : self.start,
      'end' : self.end,
      'step' : self.step,
      'values' : list(self),
    }

  def getDatetimeOfDataPoint(self, bucketId, tzinfo):
    if(isinstance(self.step, list)):
      return datetime.fromtimestamp(self.start + sum(self.step[:bucketId]), tzinfo)
    else:
      return datetime.fromtimestamp(self.start + (bucketId * self.step), tzinfo)

  def getNumberOfDataPoint(self, startTime, endTime, timeRange):
    if(isinstance(self.step, list)):
      timestamps = [ts for ts in self.step if ts >= startTime and ts <= endTime]
      return len(timestamps)
    else:
      return timeRange / self.step

  # We reduce resolution if there is too many point
  def getTimeStampsOfDataPoint(self, startTime, endTime, timeRange, maxDataPoints):
    if(isinstance(self.step, list)):
      numberOfDataPoints = self.getNumberOfDataPoint(startTime, endTime, timeRange)
      if not (maxDataPoints is None) and maxDataPoints < numberOfDataPoints:
        return timestamps[:maxDataPoints]
      else:
        return timestamps
    else:
      if not (maxDataPoints is None):
        numberOfDataPoints = self.getNumberOfDataPoint(startTime, endTime, timeRange)
        if maxDataPoints < numberOfDataPoints:
          valuesPerPoint = math.ceil(float(numberOfDataPoints) / float(maxDataPoints))
          secondsPerPoint = int(valuesPerPoint * self.step)
          # Nudge start over a little bit so that the consolidation bands align with each call
          # removing 'jitter' seen when refreshing.
          nudge = secondsPerPoint + (self.start % self.step) - (self.start % secondsPerPoint)
          self.start = self.start + nudge
          valuesToLose = int(nudge/self.step)
          for r in range(1, valuesToLose):
            del self[0]
          self.consolidate(valuesPerPoint)
          return range(int(self.start), int(self.end) + 1, int(secondsPerPoint))
      return range(int(self.start), int(self.end) + 1, int(self.step))
      
  def getLastIntervalTs(self):
    if(isinstance(self.step, list)):
      return series.end - series.step[-1]
    else:
      return series.end - series.step

# Data retrieval API
def fetchData(requestContext, pathExpr):

  seriesList = []
  startTime = int( epoch( requestContext['startTime'] ) )
  endTime   = int( epoch( requestContext['endTime'] ) )

  def _fetchData(pathExpr,startTime, endTime, requestContext, seriesList):
    matching_nodes = STORE.find(pathExpr, startTime, endTime, local=requestContext['localOnly'])
    fetches = [(node, node.fetch(startTime, endTime)) for node in matching_nodes if node.is_leaf]

    for node, results in fetches:
      if isinstance(results, FetchInProgress):
        results = results.waitForResults()

      if not results:
        log.info("render.datalib.fetchData :: no results for %s.fetch(%s, %s)" % (node, startTime, endTime))
        continue

      try:
          (timeInfo, values) = results
      except ValueError as e:
          raise Exception("could not parse timeInfo/values from metric '%s': %s" % (node.path, e))
      (start, end, step) = timeInfo

      series = TimeSeries(node.path, start, end, step, values)
      series.pathExpression = pathExpr #hack to pass expressions through to render functions
      seriesList.append(series)

    # Prune empty series with duplicate metric paths to avoid showing empty graph elements for old whisper data
    names = set([ s.name for s in seriesList ])
    for name in names:
      series_with_duplicate_names = [ s for s in seriesList if s.name == name ]
      empty_duplicates = [ s for s in series_with_duplicate_names if not nonempty(s) ]

      if series_with_duplicate_names == empty_duplicates and len(empty_duplicates) > 0: # if they're all empty
        empty_duplicates.pop() # make sure we leave one in seriesList

      for series in empty_duplicates:
        seriesList.remove(series)

    return seriesList

  retries = 1 # start counting at one to make log output and settings more readable
  while True:
    try:
      seriesList = _fetchData(pathExpr,startTime, endTime, requestContext, seriesList)
      return seriesList
    except Exception, e:
      if retries >= settings.MAX_FETCH_RETRIES:
        log.exception("Failed after %i retry! See: %s" % (settings.MAX_FETCH_RETRIES, e))
        raise Exception("Failed after %i retry! See: %s" % (settings.MAX_FETCH_RETRIES, e))
      else:
        log.exception("Got an exception when fetching data! See: %s Will do it again! Run: %i of %i" %
                     (e, retries, settings.MAX_FETCH_RETRIES))
        retries += 1


def nonempty(series):
  for value in series:
    if value is not None:
      return True

  return False
