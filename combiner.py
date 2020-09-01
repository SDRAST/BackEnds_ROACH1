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
    self.scans = {}
    for dsp in self.dsplist:
      self.logger.debug("__init__: init scans for %s", dsp)
      self.scans[dsp] = {"done": False, "scan": None, "record": None}
    self.spectra_dict = {}
    self.daemon = True
    self.start()
    
  def get_data(self):
    """
    move data from processor(s) to data handler
    """
    while True:
      data = self.inqueue.get()
      self.logger.debug("get_data: got %s", data.keys())
      self.combine_data(data)
    self.join()

  def combine_data(self, result):
    """
    combine DSP records into one 2D record
    """
    name = result['name']
    scan = result['scan']
    record = result["record"]
    self.logger.debug("combine_data: %s scan %d record %d", name, scan, record)
    self.scans[name]["record"] = record
    rec_time = calendar.timegm(time.gmtime())
    self.scans[name]["time"] = rec_time
    data = result["data"]
    if data:
      self.logger.debug("combine_data: data: %s", data[:10]) 
    # create spectra_dict[scan], if needed
    if scan in self.spectra_dict:
      # not needed
      pass
    elif list(self.spectra_dict.keys()) == []:
      # empty dict.  Creat one with the current scan
      self.spectra_dict = {scan: {}}
    else:
      # add this scan to the dict
      self.spectra_dict[scan] = {}
    # create spectra_dict[scan][record], if needed
    if record in self.spectra_dict[scan]:
      # not needed
      pass
    else:
      # create an empty dict for this record
      self.spectra_dict[scan][record] = {}
    # add the data for this ROACH to this record
    self.spectra_dict[scan][record][name] = data
    if self.spectra_dict[scan][record][name]:
      self.logger.debug("combine_data: %s data is %s", name,
                      self.spectra_dict[scan][record][name][:10])
    else:
      self.logger.debug("combine_data: %s data is done", name)
    # Are all the ROACHs' data in the spectraDict structure? If so, output 
    self.logger.debug("combine_data: scan %d record %d keys: %s",
                     scan, record, list(self.spectra_dict[scan][record].keys()))
    this_record = self.spectra_dict[scan][record]
    if len(this_record) == len(self.dsplist):
      msg = {"scan": scan, "record": record, "time": rec_time,
             "data": this_record}
      self.process_data(msg)
  
  def process_data(self, data):
    """
    what to do with the data; provided by subclass
    """
    self.logger.info("process_data:"
                     " no destination specified for data %s", data.keys())

