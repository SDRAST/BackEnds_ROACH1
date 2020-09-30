"""
Python support for ROACH1 firmware
"""
import calendar
import datetime
import errno
import h5py
import logging
import numpy
import os
import Pyro5
import queue
import re
import time

from support.local_dirs import log_dir
#from MonitorControl import ActionThread
from MonitorControl.BackEnds import Backend, get_freq_array
from support.pyro import asyncio

logger = logging.getLogger(__name__)

max_spectra_per_scan = 120 # 1 h
max_num_scans = 120 # 5 d


class SAOclient(Backend):
    """
    SAO 32K-channel spectrometer client
    
    This is a DSSServer attribute item ``equipment['Backend']``

    This backend consists of four ROACH boards, each running the same firmware.
    It takes in four IF class signals and outputs digital data (spectra) for
    each channel.

    The details of this class depend on the SAO spectrometer firmware in the
    ROACH processors.

    This is a client class which uses the server controlling the spectrometer.

    Attributes::
      bandwidth:
      freqs:
      hardware:
      last_scan:
      logger:
      name:
      num_chan:
      parent:
      roachnames:
      scans:
      spectra_dict:
      titles:
    """
    num_chan = 32768
    input_names = {"sao64k-1": "SAO1",
                   "sao64k-2": "SAO2",
                   "sao64k-3": "SAO3",
                   "sao64k-4": "SAO4"}

    def __init__(self, name, inputs=None, output_names=None, hardware=None):
        """
        create an SAOclient instance

        The SAO spectrometer has four signal processors, each of which has one
        input and puts out one spectrum. The ROACH boards are assigned the same
        name as the input port from which they take the signal.  The output port
        names are assigned in the same order as the ordered list of the names
        of the Processor instances.  Note that the order of the output names
        comes from their position in the list.

        @param name : unique name for the spectrometer
        @type  name : str

        @param inputs : input ports of the spectrometer
        @type  inputs : Port instances

        @param output_names : ordered list of names for the output ports
        @type  output_names : list of str

        @param ROACHlist : ordered list of ROACH units in the spectrometer
        @type  ROACHlist : list of str
        """
        mylogger = logging.getLogger(logger.name+".SAOclient")
        Backend.__init__(self, name, inputs=inputs, output_names=output_names)
        if hardware:
            uri = Pyro5.api.URI("PYRO:backend@localhost:50004")
            self.hardware = Pyro5.api.Proxy(uri)
            try:
                self.hardware.__get_state__()
                self.hardware._pyroRelease()
            except Pyro5.errors.CommunicationError as details:
                mylogger.error("__init__: %s", details)
                raise Pyro5.errors.CommunicationError("is the SAO spec server running?")
            except AttributeError:
                # no __get_state__ because we have a connection
                pass
        else:
            self.hardware = None
            self.roachnames = ['sao64k-1', 'sao64k-2', 'sao64k-3', 'sao64k-4']
            self.bandwidth = 1020
            self.num_chan = 32768
        self.name = name
        self.logger = mylogger
        roach1 = self.roachnames[0] # for initializing
        self.logger.info("__init__: %s input channels: %s", self, self.inputs)
        self.data['num_chan'] = len(self.freqs(roach1))
        self.logger.debug("__init__: properties: %s", list(self.keys()))
        self.update_signals() # from Device super class
        # use the following to make data arrays
        self.titles = ["Frequency"] + self.roachnames
        if hardware:
          # freqs() is a hardware method
          self.freqs = self.freqlist # MHz, any ROACH software will do
        else:
          self.freqs = get_freq_array(self.bandwidth,self.num_chan)
        self.scans = {}
        if self.hardware:
          self.logger.debug("__init__: %s", self.roachnames)
          # callback handler
          self.cb_receiver = asyncio.CallbackReceiver(parent=self)
          for name in self.roachnames:
            self.logger.debug("__init__: init scans for %s", name)
            self.scans[name] = {"done": False, "scan": None, "record": None}
          self .logger.debug("__init__: scans: %s", self.scans)
          # We want a data array with axes for frequency, roachnum, scan, record
          #   for every record of every scan we want an array like this
          #          freq roach1 roach2 roach3 roach4
          #   and each row has the data for one spectrometer channel
          #   So we know that this is always a 5x32768 array
          #   I guess a dict is the easiest way to get the scan and record
          #   spectra_dict[scan][record] = data_array
          self.spectra_dict = {}          
    
    def __getattr__(self, name):
        """
        This passes unknown method and attribute requests to the server
        """
        self.logger.debug("__getattr__: checking hardware for '%s'",
                          name)
        if self.hardware is not None:
            self.hardware._pyroClaimOwnership()
            return getattr(self.hardware, name)

    @property
    def scan_finished(self):
        done = [self.scans[name]["done"] for name in self.scans]
        return all(done)

    def get_last_spectrum(self):
      """
      serve last spectrum of the ones coming in
      """
      # find the last record of the last scan
      self.logger.debug("get_last_spectrum: called")
      self.logger.debug("get_last_spectrum: spectra_dict keys: %s",
                         list(self.spectra_dict.keys()))
      last_scan = list(self.spectra_dict.keys())[-1]
      last_record = self.spectra_dict[last_scan][-1]
      self.logger.debug("get_last_spectrum: getting scan %d, record %d",
                        last_scan, last_record)
      return self.spectra_dict[last_scan][last_record]

    def _reset_scans(self):
        self.scans = {name: {
                        "done":False,
                        "scan":0
                      }
                      for name in self.scans}

    @Pyro5.api.oneway
    def start_recording(self,
                        parent=None,
                        n_accums=max_spectra_per_scan,
                        integration_time=5.0):
        """
        start a series of scans
        
        Scans are retrieved with the callback cb_receiver.finished() invokes
        by the server.  The data are put on cb_receiver.queue.
        """
        self.parent = parent
        self.integration = integration_time
        self.logger.debug("start_recording: scan of {} accums".format(n_accums))
        self.logger.debug("start_recording: integration time: %s", integration_time)
        self.hardware._pyroClaimOwnership()
        self.hardware.start(n_accums=n_accums,
                            integration_time=integration_time,
                            callback=self.cb_receiver)
        self.logger.debug("start_recording: started")

    def help(self, kind=None):
        """
        """
        if self.hardware is not None:
            if kind == "server":
                return self.hardware.server_help()
            elif kind == "backend":
                return self.hardware.backend_help()
        return "Types available: server, backend"

    def stop_recording(self):
        """
        """
        for roachname in self.roachnames:
            self.disk_monitor[roachname].terminate()

# -----------------------------------------------------------------------------

class Obsolete():
    @Pyro5.api.callback
    def start_handler(self, res):
        """
        receive and package spectra obtained from an integration by all ROACHs

        This receives spectra from ROACHs and combines them into one array.
        Each is labelled with its scan and recorded number. The spectra are
        collected in a multi-level dict keyed on scan number, record number, and
        ROACH name.

        The SAObackend sends (via SAOspecServer) messages with five items:
          status - 'done' or 'record',
          name   - the ROACH name, e.g., 'sao64k-1',
          file   - full path to the data file
          scan   - current scan number,
          accum  - current accumulation number (-1 for status 'done'),
          data   - the spectrum (status 'record') or time (status 'done')

        20190820: To avoid problem callback we do not send data but the file
        where the data are stored.
        """
        #self.last_scan = -1 # initialize the
        self.logger.debug("start_handler: 'res' keys %s", list(res.keys()))
        if type(res) == dict:
            # callback messages always have a name and a scan
            name = res['name']
            scan = res['scan']
            self.scans[name]["scan"] = scan
            if res["type"] == "done":
                # Signal the receiving software that all records are done
                self.scans[name]["done"] = res["type"]
                self.scans[name]["time"] = res["time"]
                self.logger.debug("start_handler: %s scan %d finished", name, scan)
            elif res["type"] == "record":
                record = res["record"]
                # Process a record and add it to the current record structure
                self.scans[name]["done"] = False
                self.logger.debug("start_handler: %s scan %d record %d put on queue",
                                  name, scan, record)
        else:
            raise TypeError(
                ("start_handler: input is not a dict but type {}").format())


class ObsoleteCallbackHandler(asyncio.CallbackReceiver):
    """
    Processor for returned data
    """
    def __init__(self, parent=None, queue=None):
        """
        """
        if parent:
            pass
        else:
            raise RuntimeError("CallbackHandler needs a parent")
        mylogger = logging.logger(logger.name+".CallbackHandler")
        asyncio.CallbackReceiver.__init__(self, parent=parent, queue=queue)
        self.logger = mylogger
        
    def finished(self, msg):
        """
        replace superclass method, which just puts data on queue
        """
        if type(res) != dict:
          self.logger.error("finished: cannot handle type %s data", type(res))
          return None
        else:
          # send a messages to the browser: 
          #   {"entered": True, "time": time.strftime("%Y/%j %H:%M:%S", time.gmtime())}
          #   {"type": "start", "scan": res["scan"], "record": res["record"]}
          # set some DSSServer attributes about the ROACH input signals
          self.parent.parent.get_signals()
          # decrement the spectrum count
          self.parent.parent.spectra_left -= 1
          self.logger.debug("finished: %d spectra left", self.spectra_left)
          # put the data on the FITS queue
          self.parent.parent.FITSqueue.put(res)
          self.logger.debug("finished: data put on FITS queue")

