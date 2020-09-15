"""
Combine spectra from DSPs doing the same operation in parallel

The data from a digital signal processor (DSP) is a dict with the following 
keys::

  name   - the name or ID of the DSP
  scan   - the (1-based) number of the scan being acquired
  record - the (1-based) number of the record in the scan
"""
import calendar
import logging
import queue
import time

import MonitorControl as MC 

logger = logging.getLogger(__name__)

def nowgmt():
  return time.time()+ time.altzone

class DataCombiner(MC.ActionThread):
  """
  class to combine spectra from parallel processors
  
  Atributes
  =========
    inqueue
    logger
    outqueue
    scans
    spectra_dict
  """
  def __init__(self, dsplist=None):
    """
    initialize a data combiner
    """
    if dsplist:
      self.dsplist = dsplist
    else:
      raise RuntimeError("DataCombiner needs a list of processors to service")
    mylogger = logging.getLogger(logger.name+".DataCombiner")
    MC.ActionThread.__init__(self, self, self.get_data, name="combiner")
    self.logger = mylogger
    self.inqueue = queue.Queue()
    self.timedata = {}
    self.spectra_dict = {}
    self.daemon = True
    self.start()
    
  def get_data(self):
    """
    move data from processor(s) to data handler
    """
    while True:
      data = self.inqueue.get()
      self.combine_data(data)
    self.join()

  def combine_data(self, result):
    """
    combine DSP records into one 2D record
    """
    name = result['name']
    scan = result['scan']
    record = result["record"]
    rectime = result["time"]
    self.logger.debug("combine_data: %s %s %s %s entered", 
                      name, scan, nowgmt(), record)
    if result['type'] == 'spectrum':
      data = result["data"]
      #self.logger.debug("combine_data: %s got: %s", result['name'], data[:10]) 
      # create spectra_dict[scan], if needed
      if scan in self.spectra_dict:
        # not needed
        pass
      elif list(self.spectra_dict.keys()) == []:
        # empty dict.  Creat one with the current scan
        self.spectra_dict = {scan: {}}
        self.timedata = {scan: {}}
      else:
        # add this scan to the dict
        self.spectra_dict[scan] = {}
        self.timedata[scan] = {}
      # create spectra_dict[scan][record], if needed
      if record in self.spectra_dict[scan]:
        # not needed
        pass
      else:
        # create an empty dict for this record
        self.spectra_dict[scan][record] = {}
        self.timedata[scan][record] = {}
      # add the data for this ROACH to this record
      self.spectra_dict[scan][record][name] = data
      self.timedata[scan][record][name] = rectime
      if self.spectra_dict[scan][record][name]:
        self.logger.debug("combine_data: %s %s %s stored %s", 
                          name, scan, nowgmt(), record)
      else:
        self.logger.debug("combine_data: %s %s %s new scan",
                          name, scan, nowgmt())
      # Are all the ROACHs' data in the spectraDict structure? If so, output 
      this_record = self.spectra_dict[scan][record]
      if len(this_record) == len(self.dsplist):
        this_time = self.timedata[scan][record]
        msg = {"scan": scan, "record": record, "time": this_time,
               "type": "data", "data": this_record}
        self.process_data(msg)
    else:
      self.logger.debug("combine_data: %s received %s message", name,
                        result['type'])
                        
  def process_data(self, data):
    """
    what to do with the data; provided by subclass
    """
    self.logger.info("process_data:"
                     " no destination specified for data %s", data.keys())

