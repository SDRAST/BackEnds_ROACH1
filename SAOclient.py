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
import threading
import time

from watchdog.observers import Observer  
from watchdog.events import PatternMatchingEventHandler

from support.local_dirs import log_dir
from MonitorControl import ActionThread
from MonitorControl.BackEnds import Backend, get_freq_array

logger = logging.getLogger(__name__)

max_spectra_per_scan = 120 # 1 h
max_num_scans = 120 # 5 d

class DiskMonitor(ActionThread):
    """
    Monitors HDF5 datafile for new spectra.
  
    New spectra aread and then passed to the combiner
    """
    def __init__(self, parent, roachname, filename, interval=1, queue=None):
        """
        Initialize an SAO HDF5 spectra disk file monitor
        """
        mylogger = logging.getLogger(logger.name+".DiskMonitor")
        ActionThread.__init__(self, parent,
                                    self.check_disk,
                                    name=roachname)
        self.name = roachname
        self.filename = filename
        self.interval = interval
        self.queue = queue
        self.logger = mylogger
        self.logger.debug("__init__: done for %s",self.name)
        
    def check_disk(self):
        """
        """
        while self.end_flag == False:
          current_size = os.path.getsize(self.filename)
          self.logger.debug("check_disk: %s size is %d", self.name, current_size)
          self.logger.debug("check_disk: end flag is %s", self.end_flag)
          self.logger.debug("check_disk: suspend is %s", self.thread_suspend)
          self.logger.debug("check_disk: sleep is %s", self.thread_sleep)
          self.logger.debug("check_disk: waiting %d s", self.interval)
          #time.sleep(self.interval)
          self.logger.debug("check_disk: wait done")
          size = os.path.getsize(self.filename)
          self.logger.debug("check_disk: %s size is now %d", 
                            os.path.basename(self.filename), size)
         # if there is a cue put the spectrum there
 
class MyHandler(PatternMatchingEventHandler):
    patterns = ["*.h5py"]
    def __init__(self):
        PatternMatchingEventHandler.__init__(self)
        self.logger = logging.getLogger(logger.name+".MyHandler")

    def process(self, event):
        """
        event.event_type 
            'modified' | 'created' | 'moved' | 'deleted'
        event.is_directory
            True | False
        event.src_path
            path/to/observed/file
        """
        # the file will be processed there
        print(event.src_path, event.event_type)  # print now only for degug

    def on_modified(self, event):
        self.logger.debug("on_modified: called with %s", event)
        self.process(event)

    def on_created(self, event):
        self.logger.debug("on_created: called with %s", event)
        self.process(event)

class SAOclient(Backend):
    """
    SAO 32K-channel spectrometer

    consists of four ROACH boards, each running the same firmware. It takes in
    four IF class signals and outputs digital data (spectra) for each channel.

    The details of this class depend on the SAO spectrometer firmware in the
    ROACH processors.

    This is a client class which uses the server controlling the spectrometer.

    Attributes::
      bandwidth:
      combiner:
      combinerQueue:
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
observe_with
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
        scans = {}
        if self.hardware:
          # we need to know the current status of each ROACH
          self.logger.debug("__init__: %s", self.roachnames)
          for name in self.roachnames:
            self.logger.debug("__init__: init scans for %s", name)
            scans[name] = {"done": False, "scan": None, "record": None}
          self.scans = scans
          self .logger.debug("__init__: scans: %s", self.scans)
          # We want a data array with axes for frequency, roachnum, scan, record
          #   for every record of every scan we want an array like this
          #          freq roach1 roach2 roach3 roach4
          #   and each row has the data for one spectrometer channel
          #   So we know that this is always a 5x32768 array
          #   I guess a dict is the easiest way to get the scan and record
          #   spectra_dict[scan][record] = data_array
          self.spectra_dict = {}
          # start a combiner thread
          self.init_combiner()
          # start threads to monitor disk file size
          #self.init_disk_monitors()
          self.logger.debug("__init__: creating watchdog")
          #self.observer = Observer()
          #self.event_handler = MyHandler()
          #self.observer.daemon = True
          #self.logger.debug("__init__: scheduling watchdog handler")
          #self.observer.schedule(self.event_handler, path='/usr/local/RA_data/HDF5/dss43/2019')
          #self.logger.debug("__init__: starting watchdog")
          #self.observer.start()
          #self.logger.debug("__init__: waiting")
    
    def init_disk_monitors(self):
        """
        """
        self.logger.debug("init_disk_monitors: entered")
        self.datafilenames = self.hardware.get_datafile_names()
        self.disk_monitor = {}
        for roachname in self.roachnames:
            self.disk_monitor[roachname] = DiskMonitor(
                                        self,
                                        roachname, 
                                        self.datafilenames[roachname],
                                        interval=1, 
                                        queue=self.combinerQueue)
            self.disk_monitor[roachname].daemon = True
            self.disk_monitor[roachname].start()
        self.logger.debug("init_disk_monitors: done")

    def init_combiner(self):
        """
        combines records from different ROACHs into one packet
        """
        self.logger.debug("init_combiner: called")
        self.combinerQueue = queue.Queue()
        self.combiner = ActionThread(self, self.combine_spectra,
                                           name="combiner")
        self.combiner.daemon = True
        self.combiner.start()

    def __getattr__(self, name):
        """
        This passes unknown method and attribute requests to the server
        """
        self.logger.debug("__getattr__: checking hardware for '%s'",
                          name)
        if self.hardware is not None:
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

    # @async.async_method
    def start_recording(self,
                        parent=None,
                        n_accums=max_spectra_per_scan,
                        integration_time=5.0):
        """
        start a series of scans
        """
        self.parent = parent
        self.integration = integration_time
        self.logger.debug("start_recording: scan of {} accums".format(n_accums))
        self.logger.debug("start_recording: integration time: %s", integration_time)
        self.hardware.start(n_accums=n_accums,
                            integration_time=integration_time,
                            cb=self.start_handler)
        self.logger.debug("start_recording: started")

    def new_start_recording(self,
                        parent=None,
                        n_accums=max_spectra_per_scan,
                        integration_time=5.0):
        """
        start a series of scans
        """
        self.parent = parent
        self.logger.debug("start_recording: scan of {} accums".format(n_accums))
        self.logger.debug("start_recording: integration time: %s", integration_time)
        self.hardware.set_integration(integration_time)
        for roachname in self.roachnames:
          self.logger.debug("start_recording: %s handler id is %s",
                            roachname, id(self.start_roach_handler[roachname]))
          self.hardware.start_roach(roachname,
                                    n_accums=n_accums,
                                    cb=self.start_roach_handler[roachname])
        self.logger.debug("start_recording: started")

    @Pyro5.api.callback
    def inner_handler(self, res):
        """
        There is one of these for each ROACH
        """
        self.logger.debug("start_roach_handler: invoked for args=%s", list(res.keys()))
        self.logger.debug("start_roach_handler: handler ID=%s", id(self))
        if type(res) == dict:
            # callback messages always have a name and a scan
            name = res['name']
            #if name != self.rname:
            #  raise RuntimeError("start_roach_handler['"+self.rname+"'] got data for "+name)
            scan = res['scan']
            self.scans[name]["scan"] = scan
            if res["type"] == "done":
                # Signal the receiving software that all records are done
                self.scans[name]["done"] = res["type"]
                self.scans[name]["record"] = None
                self.scans[name]["time"] = res["time"]
                self.logger.debug("start_roach_handler: %s scan %d finished", name, scan)
            elif res["type"] == "record":
                # Process a record and add it to the current record structure
                self.scans[name]["done"] = False
                #self.combinerQueue.put(res)
                self.logger.debug("start_roach_handler: %s scan %d record %d put on queue",
                                  name, scan, res['record'])
        else:
            raise TypeError(
                ("start_roach_handler: input is not a dict but type {}").format())
    # end of def inner_handler


    #@async.async_callback
    def startRoachHandler(self, rname):
      """
      We need a method in which the ROACH name is not an argument
      """
      self.logger.debug("startRoachHandler: %s handler id is %s", rname, id(self.inner_handler))
      #self.rname = rname
      return self.inner_handler

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
                self.combinerQueue.put(res)
                self.logger.debug("start_handler: %s scan %d record %d put on queue",
                                  name, scan, record)
        else:
            raise TypeError(
                ("start_handler: input is not a dict but type {}").format())

    def get_spectrum_from_file(self, scan, res):
        """
        """
        datafile = res['file']
        self.logger.debug('get_spectrum_from_file: data file is %s',
                          datafile)
        busy = True
        topgroup = None
        while busy:
            try:
                topgroup = h5py.File(datafile, 'r')
                busy = False
            except IOError as e:
                if re.search("errno = 11", str(e)):
                    print((datetime.datetime.now().strftime("%H:%M:%S.%f > ") + \
                          datafile+' is busy; waiting...'))
                    time.sleep(0.1)
                else:
                    pass
            except Exception as e:
                self.logger.error("start_handler: "+str(e))
                raise IOError('open '+datafile+' failed due to '+
                                                          os.strerror(e.errno))
                busy = False
        if topgroup:
          scangroup = topgroup["%03d" % scan] # selects scan from file
          record = res["record"]
          self.logger.debug("get_spectrum_from_file: getting record %s", record)
          # DEFERRED: this should be checked gainst the value brought in
          rectime = scangroup['timestamp'][record]
          self.logger.debug("get_spectrum_from_file: record time is %s", rectime)
          # add the data back in
          res['data'] = scangroup['data'][record] # selects record from scan
          self.logger.debug("get_spectrum_from_file: data is %s", res['data'])
          topgroup.close()
        return res

    def combine_spectra(self):
        """
        combine ROACH recors into a spectrometer record
        """
        result = self.combinerQueue.get()
        name = result['name']
        scan = result['scan']
        record = result["record"]
        self.logger.debug("combine_spectra: %s scan %d record %d", name, scan, record)
        self.scans[name]["record"] = record
        self.scans[name]["time"] = calendar.timegm(time.gmtime())
        result = self.get_spectrum_from_file(scan, result)
        self.logger.debug("combine_spectra: retrieved %s", result)
        data = result["data"]
        self.logger.debug("combine_spectra: data: %s", data) 
        
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
        self.logger.debug("combine_spectra: %s data is %s", name, self.spectra_dict[scan][record][name])
        # Is this record complete? Are all the ROACHs' data in the
        # spectraDict structure? If so return structure
        self.logger.debug("combine_spectra: scan %d record %d keys: %s",
                          scan, record, list(self.spectra_dict[scan][record].keys()))
        if len(self.spectra_dict[scan][record]) == len(self.roachnames):
          # yes
          # now make the 2D list of lists
          npts = len(self.freqs)
          # first column
          spectra = numpy.array(self.freqs).reshape(npts,1)
          self.logger.debug("combine_spectra: initialize spectra: shape is %s",
                            spectra.shape)
          # add other columns
          # does this column and record exist for all ROACHs?
          self.logger.debug("combine_spectra: adding data for %s", self.roachnames)
          for name in self.roachnames:
            try:
              column_data = self.spectra_dict[scan][record][name]
              self.logger.debug("combine_spectra: %s data is %s", name, self.spectra_dict[scan][record][name])
              spectrum = numpy.array(column_data)
              self.logger.debug("combine_spectra: %s spectrum is %s", name, spectrum)
              self.logger.debug("combine_spectra: %s spectrum shape is %s", name, spectrum.shape)
              spectra = numpy.append(spectra, spectrum.reshape(npts,1), 1)
              self.logger.debug("combine_spectra: spectra shape is %s", spectra.shape)
            except ValueError as err:
              self.logger.error(
                "combine_spectra: cannot combine data from ROACH {}: {}".format(name, err))
          try:
            self.parent.start_spec_cb_handler({"type":   "data",
                                               "scan":   scan,
                                               "record": record,
                                               "titles": [["Frequency"] + self.roachnames],
                                               "data":   spectra.tolist()})
          except AttributeError:
            # no callback specified
            self.logger.info("combine_spectra: start_spec_cb_handler not called")
          self.logger.debug("combine_spectra: data sent")
        else:
          # self.parent.start_spec_cb_handler("hello")

          self.logger.info("combine_spectra: scan %d record %d has only %s",
                              scan, record, list(self.spectra_dict[scan][record].keys()))

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
        self.combiner.terminate()
        for roachname in self.roachnames:
            self.disk_monitor[roachname].terminate()

