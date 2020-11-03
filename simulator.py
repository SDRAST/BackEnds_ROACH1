# -*- coding: utf-8 -*-
"""
This simulates a multi-ROACH backend using the TAMS firmare.

The basic program behavior is to create a ``SAObackend`` object.  This is the
parent for ``SAOfwif`` (SAO firmware interface) objects, one for each ROACH. The
``SAOfwif`` class is a subclass of the ``DeviceReadThread`` class.

Each ``SAOfwif`` object has one or more ``Channel`` objects (one for ROACH1, 
four for ROACH2) which convert IF signals into 32K power spectra.

Because conversion to 4-channel firmware is pending, a number of ``SAOfwif``
methods should be eventually converted to ``SAOfwif.Channel`` methods.

Each ``SAOfwif`` object initializes its channel(s) and simulates the ROACH's
firmware initialization which consists of::

  * setting the FFT shift register,
  * setting the gain of the RF sections of the ADC channels,
  * setting the accumulation (integration) time as a number of accumulations,
  * configures the firmware,
  * starts the thread, providing it with an 'action' method,
  * immediately suspends the thread,
  * sets the scan number to 1.

The ``SAObackend'``method ``start()`` is invoked by a client. For each ROACH it::

  * resumes the thread,
  * invokes the client's callback handler to provide the start time,
  * sets the number of accumulated spectra to zero.

The ``SAOfwif`` method ``action()`` increments the number of accumulations and
checks to see if it exceeds exceeds the specified number of accumulations for
a scan.  If not it::

  * waits for the accumulation to be completed,
  * puts the accumulation (spectrum) on the combiner queue.
  
If the number of accumulations exceeds the specified maximum it::

  * suspends the threat,
  * sets the number of accumulations to 0,
  * increments the scan number.

Note therefore that a scan consists of some specified number of accumulations
(integrations) each of which is written as a record, that is, as a row in a 2D
numpy array.  It is the job of the client to start a new scan which resumes the
thread.


*Scan and Record Numbers*

Scan and record numbers are integers or three digit strings starting with "001". 
Each ROACH has its own copy of the scan number. 

Record numbers are initialized to 0 when the SAOfwif object is created. It is 
incremented each time ``action()`` is entered. 

Scan numbers are initialized as 0.  The The scan numbers are updated in the 
``SAObackend`` method ``action()`` when a scan is completed. (It's better to 
think of ``action()`` as an ``SAOfwif`` method provided by the parent 
``SAObackend``.)

*Metadata*

The SAOfwif object creates an HDF5 file when it is initialised. The firmware
register data which do not change are attrs of the top level of the HDF5
hierarchy. The SAObackend method ``start(N)`` will start a new scan of N records
for each ROACH. Scans are at the second level of the HDF5 hierarchy. The attrs
of the scan level of the file are those firmware registers which do not change
during a scan. Each record is written to disk as it is acquired. The attrs of
the record level are the register values which change all the time, and also
time in seconds.

Example
=======

Creating a backend server at the command line::

  from MonitorControl.BackEnds.ROACH1.SAOfwif import SAObackend
  be = SAObackend("SAO spectrometer",
                  roachlist=["sao64k-1", "sao64k-2", "sao64k-3", "sao64k-4"])

"""

import calendar
import datetime
import logging
import math
import numpy
import os.path
import Pyro5
import scipy.stats as stats
import socket
import time

import MonitorControl as MC
import MonitorControl.BackEnds as BE
import MonitorControl.BackEnds.ROACH1 as ROACH1
import MonitorControl.BackEnds.ROACH1.combiner as combiner
import MonitorControl.BackEnds.ROACH1.firmware_server as fws
import Radio_Astronomy as RA
import support
import support.local_dirs
import support.pyro.async_method_new as async_method
import support.pyro.pyro5_server

logger = logging.getLogger(__name__)
fws.module_logger.setLevel(logging.WARNING)

modulepath = os.path.dirname(os.path.abspath(__file__))
paramfile = "model_params.xlsx"
max_spectra_per_scan = 120 # 1 h at 5 s per spectrum
T_sys = 60

def nowgmt():
  return time.time()+ time.altzone

def logtime():
  return datetime.datetime.utcnow().strftime("%H:%M:%S.%f")[:-3]


  
################################ classes #################################

class RoachCombiner(combiner.DataCombiner):
  """
  subclass with meaningful ``process_data`` method
  """
  def __init__(self, parent=None, dsplist=None):
    """
    initialize a DataCombiner
    """
    combiner.DataCombiner.__init__(self, dsplist=dsplist)
    self.parent = parent
    self.logger = logging.getLogger(logger.name+".RoachCombiner")
    self.callback = None
  
  def process_data(self, msg):
    """
    replaces method in superclass
    """
    self.logger.debug("process_data: got %s items", len(msg))
    self.logger.debug("process_data: callback is %s", self.parent.start.cb)
    self.callback = self.parent.start.cb
    self.caller = self.parent.start.caller
    if self.callback:
      self.logger.debug("process_data: callback is %s", self.callback)
      # claim method from another thread
      self.caller._pyroClaimOwnership()
      # invoke callback's method
      #self.callback.finished(msg)
      self.callback(msg)
    else:
      self.logger.error("process_data: no callback specified") 
    
@Pyro5.server.expose
class SAObackend(support.PropertiedClass):
  """
  A simulated multi-ROACH 32k-channel x 4-IF spectrometer

  An SAO back end normally comprises four ROACH boards using TAMS 32K channel
  firmware.  We will generalize it, though, to use any number of ROACH boards.

  Attributes::

    logger       - logging.Logger instance
    name         - name for the backend
    reader       - dict of DeviceReadThread objects keyed to roach names
    roach        - dict of SAOfwif objects keyed to their names

  The backend manages the scans for the child ROACH objects.  Their scan
  numbers are updated when the required number of records have been recorded.

  Typically, the SAO client is an attribute of the main client so that the
  server's methods are called with::
  
    client.spectrometer.hardware.method()
  """
  def __init__(self, name, roaches={},
                     roachlist=['roach1', 'roach2', 'roach3', 'roach4'], 
                     template='roach',
                     synth=None, write_to_disk=False, TAMS_logging=False):
    """
    Initialise a multi-IF high-res spectrometer.

    It is intended that the class be told what ROACH boards to use but by
    default it will use what it finds according to the name template provided,
    in alphanumeric order.  In the absence of a template it will try 'roach'
    and 'sao' as templates.

    @param name : name for the backend
    @type  name : str

    @param roaches : optional list of TAMS-programmed ROACHes
    @type  roaches : list of SAOfwif instances

    @param roachlist : list of unitialized ROACHs by hostname; default: all
    @type  roachlist : list of str

    @param template : template for finding ROACHes in /etc/hosts
    @type  template : str

    @param synth : a synthesizer object
    """
    mylogger = logging.getLogger(logger.name + ".SAObackend")
    support.PropertiedClass.__init__(self)
    self.name = name # this may be a problem with Pyro
    self.logger = mylogger
    self._template = template
    # firmware details
    firmware_server = fws.FirmwareServer(modulepath, paramfile)
    # ROACH firmware interface objects
    self.roach = {}
    # make ordered list of ROACH names as keys
    for name in roachlist:
      # make unitialized ROACH
      roaches[name] = None
    self._roachkeys = list(roaches.keys())
    self._roachkeys.sort()
    # input state of each ROACH
    self.rf_enabled = {}
    # last spectrum from each ROACH
    self.spectrum = {}
    # a combiner collects records from all the ROACH for client callback
    self.callback = None
    self.combiner = RoachCombiner(parent=self, dsplist=self._roachkeys)
    for name in self._roachkeys:
      # arguments to pass when initializing a SAOfwif object
      init = dict(parent=self,
                  roach=name,
                  template=self._template,
                  firmware_server=firmware_server,
                  firmware_key='sao_spec',
                  roach_log_level=logging.INFO,
                  clock_synth=synth,
                  TAMS_logging=TAMS_logging)
      # initialize each ROACH
      self.roach[name] = SAOfwif(**init)
      self.roach[name].scan = 0
      # each ROACH has one RF inputs of unknown state
      self.rf_enabled[name] = {0: {0: None}}
      # get the ROACH input state
      self.rf_state(name)
      # ROACH spectra initially empty
      self.spectrum[name] = {}
    # convenience  -- assume all ROACHs the same
    roach = self.roachnames[0]
    self._firmware = self.get_firmware(roach)
    self._bandwidth = self.get_bandwidth(roach)
    self._bitstream = self.roach[roach].bitstream
    self.logger.debug("__init__: completed for %s", self.name)

  @property
  def bandwidth(self):
    return self._bandwidth

  @property
  def firmware(self):
    return self._firmware

  @property
  def bitstream(self):
    return self._bitstream

  @property
  def roachnames(self):
    return self._roachkeys

  @property
  def freqlist(self):
    return list(self.freqs(self.roachnames[0]))
    
  @property
  def template(self):
    return self._template

  def set_integration(self, int_time):
    """
    Sets all ROACHes to the same integration time.
    """
    self.integr_time = int_time
    for name in list(self.roach.keys()):
      self.roach[name].integr_time_set(integr_time=int_time)

  @Pyro5.api.oneway
  def quit(self):
    """
    """
    for roach in list(self.roach.keys()):
      self.roach[roach].quit()

  def get_current_scans(self):
    """
    report the current scan number for each ROACH
    """
    scans = {}
    for name in list(self.roach.keys()):
      scans[name] = self.roach[name].scan
    return scans

  def get_current_accums(self):
    accums = {}
    for name in list(self.roach.keys()):
      accums[name] = self.roach[name].spectrum_count
    return accums

  def help(self):
    """
    """
    return get_help(self.__class__)
  
  @async_method.async_method # @Pyro5.api.oneway
  def start(self, n_accums=max_spectra_per_scan, integration_time=10.0):
    """
    start a scan consisting of 'n_accums' accumulations

    Adapted from SAObackend.start and SAObackend.action.  This decorated 
    `oneway` so its not return a result and won't hold up the calling
    thread.
    """
    self.logger.debug("start: called for %d accumulations", n_accums)
    #self.logger.debug("start: callback is %s", self.start.cb)
    #self.combiner.callback = self.start.cb
    self.set_integration(integration_time)
    for name in list(self.roach.keys()):
      self.logger.debug("start: starting %s", name)
      if self.roach[name].scan == 0:
        self.roach[name].scan = 1
      self.roach[name].max_count = n_accums
      self.roach[name].spectrum_count = 0 # so first one is '1'
      self.roach[name].sync_start()

  #@Pyro5.api.oneway
  def last_spectra(self, dolog=True, squish=16):
      """
      Get the current spectrum from each ROACH

      Returns a list of lists that is compatible with Google Charts LineChart

      We cheat a little in assuming that each ROACH has the same scan and
      spectrum numbers.

      Arguments:
        dolog - return log10 of data if True; negative number become 0
        squish - number of channels to average
      """
      self.logger.debug("last_spectra: called")
      if squish > 1:
        pass
      else:
        # may not be 0 or negative
        squish = 16
      names = list(self.roach.keys())
      names.sort()
      titles = ["Frequency"] + names
      npts = int(self.roach[names[0]].freqs.shape[0]/squish)
      freqs = BE.get_freq_array(self.roach[names[0]].bandwidth, npts)
      # data have a typical spreedsheet organization, one list per line
      # this is the first column
      spectra = freqs.reshape(npts,1) # converts from 1D to 2D array
      # for each ROACH, one column for each ROACH
      for name in names:
          self.logger.debug("last_spectra: %s", name)
          spectrum = self.roach[name].get_spectrum()
          self.logger.debug("last_spectra: got %d samples", len(spectrum))
          # average over 'squish' channels
          
          result = spectrum.reshape(spectrum.shape[0]//squish, squish)
          result = result.mean(axis=1)
          self.logger.debug("last_spectra: converted to %s array",
                             result.shape)
          if dolog:
            result = numpy.log10(result)
            result[result==-numpy.inf]=0
          column = result.reshape(npts,1)
          self.logger.debug("last_spectra: column shape is %s", column.shape)
          spectra = numpy.append(spectra, column, axis=1)
      final_list = [titles] + spectra.tolist()
      self.logger.debug("last_spectra: got %s", final_list[0:5])
      return {"scan":   self.roach[names[0]].scan,
              "record": self.roach[names[0]].spectrum_count,
              "table":  final_list}

  def reset_scans(self):
    for name in self.roach:
      self.roach[name].scan = 0

    
  # The following methods invoke individual ROACH methods.  This is to make
  # individual ROACHs accessible to the client.
  
  @ROACH1.roach_name_adaptor
  def freqs(self, roachname):
    """
    returns the channel frequencies

    Example::
      In [5]: k.freqs('roach1')
      Out[5]: array([  0.00000000e+00,   6.34765625e-01,   1.26953125e+00, ...,
                       6.48095703e+02,   6.48730469e+02,   6.49365234e+02])
    """
    return list(self.roach[roachname].freqs)

  @ROACH1.roach_name_adaptor
  def get_firmware(self, roachname):
    """
    returns firmware loaded in specified ROACH
    Example::
      In [6]: k.get_firmware('roach2')
      Out[6]: 'kurt_spec'

    'firmware' is now an attribute, not a method 2019-06-25
    """
    return self.roach[roachname].firmware

  @ROACH1.roach_name_adaptor
  def get_bandwidth(self, roachname):
    """
    returns the spectrometer bandwidth

    Example::
      In [3]: k.get_bandwidth('roach1')
      Out[3]: 650
    'bandwidth' is now a property 2019-06-25
    """
    return self.roach[roachname].bandwidth

  @ROACH1.roach_name_adaptor
  def rf_gain_get(self, roachname, ADC=0, RF=0):
    """
    returns the gain of the specified RF channel

    Example::
      In [17]: k.rf_gain_get('roach1', 1)
      Out[17]: 20.0
    """
    # this updates the gain info for the designated roach
    self.roach[roachname].RFchannel[RF].rf_gain_get()
    # this returns the gain value
    return self.roach[roachname].RFchannel[RF].rf_gain

  @ROACH1.roach_name_adaptor
  def rf_state(self, roachname, ADC=0, RF=0):
    """
    returns whether the RF section is enabled or not

    Example::
      In [17]: k.rf_state('roach1')
      Out[17]: True
    """
    # this updates the gain info for the designated roach
    self.roach[roachname].RFchannel[RF].rf_gain_get()
    # this returns the enabled state
    state = self.roach[roachname].RFchannel[RF].rf_enabled
    self.rf_enabled[roachname][0][0] = state
    return state

  @ROACH1.roach_name_adaptor
  def rf_gain_set(self, roachname, ADC=0, RF=0, gain=0):
    """
    returns the gain of the specified RF channel

    Example::
      In [17]: k.rf_gain_set('roach1', RF=1, gain=20)
      Out[17]: 20.0
    """
    # this updates the gain info for the designated roach
    self.roach[roachname].RFchannel[RF].rf_gain_set(gain=gain)
    # this returns the gain value
    return self.roach[roachname].RFchannel[RF].rf_gain

  @ROACH1.roach_name_adaptor
  def get_ADC_samples(self, roachname, RF=0):
    """
    returns ADC samples for specific ROACH and ADC input
    """
    self.logger.debug("get_ADC_samples: called for %s ADC %d", roachname, RF)
    self.logger.debug("get_ADC_samples: device is %s",
                      self.roach[roachname].RFchannel[RF])
    return self.roach[roachname].RFchannel[RF].ADC_samples()

  @ROACH1.roach_name_adaptor
  def get_ADC_input(self, roachname, RF=0):
    """
    returns the power levelinto the ADC

    Example::
      In [21]: k.get_ADC_input('roach1',0)
      Out[21]: {'Vrms ADC': 0.062617406666096623,
                'W ADC': 7.8418792351746437e-05,
                'dBm ADC': -11.05579850112974,
                'sample mean': -0.987548828125,
                'sample std': 15.9331823577854}
    """
    self.logger.debug("get_ADC_input: called for %s ADC %d", roachname, RF)
    self.logger.debug("get_ADC_input: device is %s",
                      self.roach[roachname].RFchannel[RF])
    return self.roach[roachname].RFchannel[RF].get_ADC_input()

  # methods for firmware

  @ROACH1.roach_name_adaptor
  def read_register(self, roachname, register):
    """
    returns register contents
    """
    return self.roach[roachname].read_register(register)

  # miscellaneous methods

  @ROACH1.roach_name_adaptor
  def check_fans(self, roachname):
    """
    check the speed of the chassis fans
    """
    response = self.roach[roachname].check_fans()
    self.logger.info("check_fans:  %s", response)
    return response

  @ROACH1.roach_name_adaptor
  def check_temperatures(self, roachname):
    """
    returns physical temperatures on the ROACH board
    """
    response = self.roach[roachname].get_temperatures()
    self.logger.info("check_temperatures:  %s", response)
    return response

  @ROACH1.roach_name_adaptor
  def clock_synth_status(self, roachname):
    """
    Returns the status of the sampling clock

    Example::
      In [27]: k.clock_synth_status('roach2')
      Out[27]: {'VCO range': (2200, 4400),
                'frequency': 1300.0,
                'label': 'Synthesizer B   ',
                'options': (0, 1, 10, 0),
                'phase lock': False,
                'rf_level': 5}
    """
    return self.roach[roachname].clock_synth.update_synth_status()

  @ROACH1.roach_name_adaptor
  def initialize(self, roachname):
    """Initialize the ROACH to default values."""
    self.logger.info("Initializing ROACH {}".format(roachname))
    self.roach[roachname].initialize_roach()

  @ROACH1.roach_name_adaptor
  def calibrate(self, roachname):
    """Calibrate the ROACH ADC."""
    self.logger.info("Calibrating ROACH {}".format(roachname))
    self.roach[roachname].calibrate()

  @ROACH1.roach_name_adaptor
  def fft_shift_set(self, roachname, val):
    """Set the fft shift for a specific FPGA."""
    self.logger.info(
              "Setting the fft shift for ROACH {} to {}".format(roachname, val))
    self.roach[roachname].fft_shift_set(val)

  @ROACH1.roach_name_adaptor
  def sync_start(self, roachname):
    """Call sync_start for specified ROACH"""
    self.logger.info(
      "Synchronizing vector accumulators for ROACH {}".format(roachname))
    self.roach[roachname].sync_start()

  @ROACH1.roach_name_adaptor
  def get_clk(self, roachname):
    self.logger.info("get_clk: ROACH {}".format(roachname))
    clk = self.roach[roachname].get_clk()
    return clk

  @ROACH1.roach_name_adaptor
  def get_adc_temp(self, roachname):
    self.logger.info("get_adc_temp: ROACH {}".format(roachname))
    self.logger.warning("get_adc_temp: the SAObackend.get_temperatures method is prefered")
    adc_temp = self.roach[roachname].get_adc_temp()
    return adc_temp

  @ROACH1.roach_name_adaptor
  def get_ambient_temp(self, roachname):
    self.logger.info("get_ambient_temp: ROACH {}".format(roachname))
    self.logger.warning("get_adc_temp: the SAObackend.get_temperatures method is prefered")
    ambient_temp = self.roach[roachname].get_ambient_temp()
    return ambient_temp

#--  ------------------------------- SAOfwif------------------------------------

@Pyro5.server.expose
class SAOfwif(MC.DeviceReadThread):
  """
  Class for one ROACH with SAO 32K spectrometer firmware

  Attributes::
  
    spectrum_count - current accumulation number
    gains     - RF gain (only 1 ADC and 1 RF input)
    data_file_obj  - where raw data are stored
    logger    - logging.Logger instance
    max_count - number of accumulation in a scan
    rf_gain   - gain of RF input
    scan      - number of the current group of accumulations
    
  Attributes inherited from SAOhwif::
  
    fft_shift - contents of register fft_shift
    fpga      - ROACH hardware
  """
  command_help = """
    ADC_samples(trig_level=-1, timeout=1)
       Returns ADC samples.
    integr_time_set(integr_time=1)
       Set the accumulation length in seconds
    ctrl_get()
       Reads and decodes the values from the control register.
    ctrl_set(**kwargs)
       Set control register bits.

       This is designed to do the minimum number of writes to the 'control'
       register, allowing bits simultaneously to be set, unset, toggled and
       pulsed.
    fft_shift_set(fft_shift_schedule=0)
       Sets the FFT shift schedule (divide-by-two) on each FFT stage.
    get_ADC_input()
       Get the ADC input level properties and a set of samples
    get_RF_input()
       Get the mean RF section input level. This is the ADC level minus the
       gain of the RF section
    get_accum_count()
    get_gains()
       Get the gains of the RF channels. This only works for ADC type 'katadc'.
       The gain is set by a 20 dB amplifier followed by a -31.5 to 0 dB
       attenuator controllable in 0.5 dB steps.  So the actual gain goes from
       -11.5 dB to +20 dB in 0.5 dB steps.
    get_next_spectrum()
       invokes ``get_spectrum`` to get the spectrum from the spectrometer
    get_spectrum()
    initialize_roach(RF_gain=10, integr_time=1)
       Initialises the system to defaults.
    quit()
    read_fpga_uscram(store=0)
    rf_gain_set(gain=20)
       Enables the RF switch and configures the RF attenuators.
       For KATADC boards. KATADC's valid range is -11.5 to 20dB. The RF switch
       is in MSb.
    write_to_data_file(data)
       Save a record
    sync_start()
       Initiate the sync pulses

    Methods inherited from MonitorControl.BackEnds.ROACH1.ROACHhwif.ROACHhwif:

    get_params(key)
       Look up the parameters for the design identified by the key
    get_register_values()
       Report on the contents of regular registers.
    listdev()
       List the devices defined in the firmware.
    report()
       Get a summary of information for this ROACH

    Methods inherited from MonitorControl.BackEnds.ROACH1.ROACHhwif.ROACHppc:

    check_borph_status()
       See if the borphserver is running bormally and fix it if not.
    get_bitfile()
       What boffile is running?
    get_boffiles(filter_tutorials=True)
       What boffiles does it have?
    get_firmware_ID()
       Get the firmware IDs of the currently loaded boffiles
    get_firmware_keys()
       Sets (and returns) the names of the firmwares available on this host
    is_alive()
       Is it alive?
    roach_command(self, command)
       Send a command to the ROACH PPC."""

  max_data_file_size = 1e9
  file_attr_keys = ['sys_board_id', 'sys_rev', 'sys_rev_rcs']
  scan_attr_keys = ['control', 'fft_shift', 'adc_ctrl0', 'acc_len']
  accum_reg_keys = ['status', 'sync_start', 'sys_scratchpad', 'acc_cnt',
                    'sync_cnt', 'sys_clkcounter']

  def __init__(self, parent          = None,
                     roach           = 'roach1',
                     template        = 'sao',
                     firmware_server = None,
                     firmware_key    = "sao_spec",
                     port            = None,
                     roach_log_level = logging.INFO,
                     clock_synth     = None,
                     integr_time        = 1,
                     write_to_disk   = False,
                     TAMS_logging    = False,
                     timing_report   = False):
    mylogger = logging.getLogger(logger.name + ".SAOfwif")
    mylogger.debug("__init__: initializing %s", roach)
    # the first argument (after 'self') is the object providing 'action'
    MC.DeviceReadThread.__init__(self, self, self.action, name=roach+"-reader",
                                 suspend=True)
    mylogger.debug("__init__: initializing %s hardware", roach)
    self.name = roach
    self.parent = parent
    self.firmware = firmware_key
    if firmware_server:
      self.firmware_server = firmware_server
    else:
      raise RuntimeError("a firmware server is required")
    self.logger = mylogger
    self.get_params()
    self.freqs = BE.get_freq_array(self.bandwidth, self.num_chan)
    self.RFchannel = {0: SAOfwif.Channel(self, "RF0")}
    # integration (number of accumulations)
    self.spectrum_count = 0
    # The following is over-written by start().  The low value here is for the
    # first call to ``action()`` after the thread is created but not yet
    # suspended.
    self.max_count = 1
    # initialize
    self.initialize_roach(integr_time=integr_time, timing_report=timing_report)
    self.daemon = True
    # action will ignore ``scan=0`` until the thread is suspended
    self.scan = 0
    self.start() # this starts the thread
    #self.suspend_thread()

  def initialize_roach(self, RF_gain=0, RFid=0, integr_time=1,
                       timing_report=False):
    """
    Initialises the system to defaults.

    For ROACH2 it will be necessary to loop over RF channels.
    """
    self.logger.debug("initialize_roach: entered for %s", self.name)
    # FFT shift comes from firmware spreadsheet
    self.fft_shift_set(self.fft_shift)
    self.logger.info("initialize_roach: %s fft shift = %s",
                     self.name, bin(self.fft_shift))
    # default gain is 10 dB
    self.RFchannel[0].rf_gain_set(RF_gain)
    self.get_gains() # allows for multiple channels
    self.logger.info("initialize_roach: %s has %4.1f dB gain; enabled is %s",
                     self.name, self.gains[RFid]['gain'], 
                     self.gains[RFid]['enabled'])
    # default integration time per accumulation is 1 sec
    self.integr_time_set(integr_time)
    self.ctrl_set(flasher_en=False, cnt_rst='pulse', clr_status='pulse')
    self.get_gains()
    self.logger.debug("initialize_roach: %s done", self.name)

  def action(self):
    """
    This provides the action for the child readers.

    The 'DeviceReadThread' objects created during initialization invoke their
    parents' (this object) 'action' method.

    It keeps track of the number of spectra in a scan. It saves the data with
    associated header values for each spectrum.  When the number of
    spectra equals the requested number, it stops and reports.
    """
    # record number, starts with 0 so first one is 1
    self.spectrum_count += 1
    UNIXtime = nowgmt()
    self.logger.debug(
               "action: %s %s %s %s entered",
               self.name, self.scan, logtime(), self.spectrum_count)
    if self.spectrum_count > self.max_count:
      # got all spectra for this scan
      self.suspend_thread()
      self.logger.info("action: %s %d %s done", self.name, self.scan, logtime())
      # increment scan
      self.scan += 1
      # start a new scan
      self.spectrum_count = 0  # reset counter
      msg = {"type": "new scan", 
             "name": self.name,
             "time": UNIXtime, 
             "scan": self.scan,
             "record": 0,
             "data": None}
      self.logger.debug("action: %s %s %s new scan to combiner", 
                        self.name, self.scan, logtime())
      self.parent.combiner.inqueue.put(msg)
    else:
      # Get another integration (accumulation)
      #    this blocks until the spectrum is done
      accum = self.get_next_spectrum()
      self.logger.debug("action: %s %s %s got integration %d", 
                        self.name, self.scan, logtime(), self.spectrum_count)
      msg = {"type":"spectrum", 
             "name": self.name, 
             "time": UNIXtime,
             "scan": self.scan, 
             "record": self.spectrum_count, 
             "data": list(accum)}
      self.parent.combiner.inqueue.put(msg)
      self.logger.debug("action: %s %s %s finished %s",
                        self.name, self.scan, logtime(), self.spectrum_count)
        
  def calibrate(self):
    """
    Calibrate the FPGA.
    """
    pass
  
  def get_ambient_temp(self):
    """Get the ambient ADC temp"""
    return 40.

  def get_adc_temp(self):
    """Get the adc temp"""
    return 70.

  def get_clk(self):
    """Get an estimate of the FPGA clock speed."""
    return 640.
  
  def sync_start(self):
    """
    Initiate the sync pulses
    """
    self.end_integr = nowgmt() + self.integr_time
    self.logger.debug("sync_start: %s will stop at %s", 
                      self.name, self.end_integr)
    self.resume_thread()
    
  def fft_shift_set(self, fft_shift_schedule=int(0b0000000000000000)):
    """
    Sets the FFT shift schedule (divide-by-two) on each FFT stage.

    Input is an integer representing a binary bitmask for shifting.
    If not specified as a parameter to this function (or a negative
    value is supplied), it programs the default level.
    """
    pass
  
  def get_gains(self):
    """
    Get the gains of the RF channels

    @return: dict of gains[ADC][RF]
    """
    self.gains = {}
    for RFid in list(self.RFchannel.keys()):
      self.RFchannel[RFid].rf_gain_get()
      self.gains[RFid] = {'enabled': self.RFchannel[RFid].rf_enabled,
                          'gain': self.RFchannel[RFid].rf_gain}
    return self.gains

  def integr_time_get(self):
    """
    Get the accumulation length.
    """
    return self.integr_time

  def integr_time_set(self, integr_time=1):
    """
    Set the accumulation length in seconds
    
    Each spectrum is taken from ``num_chan`` samples acquired at a rate of
    ``bandwidth*1e6`` samples per second. This times ``integr_time`` is the
    number of raw spectra in one spectrum. This number is written to
    ``acc_len``. Once the spectrometer is started, it will collect spectra until
    this number is reached.  Register ``acc_cnt`` is then incremented.
    """
    self.integr_time = integr_time
    self.raw_per_sec = int(round(float(self.bandwidth * 1e6) / self.num_chan))
    self.n_raw = int(round(integr_time * self.raw_per_sec))
    self.logger.info(
      "integr_time_set: %s accum. time set to %2.2f sec (%i raw spectra).",
      self.name, integr_time, self.n_raw)
    #self.sync_start()

  def ctrl_set(self, **kwargs):
    """
    Set control register bits.
    """
    pass

  def ctrl_get(self):
    """
    Reads and decodes the values from the control register.
    """
    pass

  def read_register(self, register):
    """
    To allow a client to read register
    """
    pass

  def ADC_samples(self, trig_level=-1, timeout=1):
    """
    Returns ADC samples.
    """
    samples = self.RFchannel[0].get_ADC_snap()
    return samples

  def get_ADC_input(self):
    """
    Get the ADC input level properties and a set of samples
    
    This is input to the chip
    """
    samples = self.ADC_samples()
    if samples is None:
      self.logger.warning(
        "get_ADC_input: ADC_samples() needs to be defined in a subclass")
      return None
    else:
      level = {}
      level["sample mean"] = samples.mean()
      level["sample std"] = samples.std()
      self.logger.debug("get_ADC_input: %5.1f +/- %5.1f",
                        level["sample mean"],
                        level["sample std"])
      level["Vrms ADC"] = level["sample std"] * adc_cnt_mv_scale_factor() / 1000
      level["W ADC"] = volts_to_watts(level["Vrms ADC"])
      level["dBm ADC"] = v_to_dbm(level["Vrms ADC"])
      return level, samples

  def get_RF_input(self):
    """
    Get the mean RF section input level.

    This is the ADC level minus the gain of the RF section
    """
    level = self.get_ADC_input()[0]
    if level is not None:
      # Kurtosis spectrometer has only one ADC in ZDOC 0.
      dBgain = self.gains[0][0]['gain']
      level["dBm RF"] = level["dBm ADC"] - dBgain
      factor = gain(dBgain)
      level["W RF"] = level["W ADC"] / factor
    return level

  def get_accum_count(self):
    """
    Returns the current number of raw spectra accumulated
    
    A single spectrum consists of an accumulation of some number of raw spectra.
    This is written into register ``acc_len``.  When this number of raw spectra
    have been accumulated, ``acc_cnt`` is incremented.

    The spectrometer does not stop running and ``acc_cnt`` will keep on 
    incrementing until the spectrometer is reset.
    """
    return self.spectrum_count

  def get_spectrum(self):
    """
    This is what read hardware would do::
    
      spec = self.read_fpga_uscram(0)[0]
      for sp in [1, 3, 2]:
        spec = numpy.append(spec, self.read_fpga_uscram(sp)[0])
    
    We simulate. We begin with one set of normalized data samples::
    
      normalized = norm.rvs(size=self.bandwidth)
    
    with a standard deviation of 1. The actual samples are integers in the range
    of -128 to 127, with some standard deviation related to the power::
    
      samples = std * norm.rvs(size=self.bandwidth)
      
    See ``Channel.get_ADC_snap()``.  The central value theorem says that we end
    up with a normal distrubtion about the mean::
    
      spectrum.mean = std *      max_count  * 2**24 / fft_shift
      spectrum.std  = std * sqrt(max_count) * 2**24 / fft_shift
    
    """
    rms_radio = RA.rms_noise(T_sys, self.bandwidth*1e6, self.integr_time)/T_sys
    sampleRMS = self.ADC_samples().std()
    specmean = sampleRMS * self.n_raw
    spec = specmean + stats.norm.rvs(sampleRMS, size=self.num_chan)
    return spec

  def get_next_spectrum(self):
    """
    Suggested by Jonathan::

      acc_new = fpga.read(acc_cnt)
      if acc_new == acc_old
          do nothing
      else if  acc_new == acc_old + 1
        read_bram
        acc_old = acc_new
      else
        missed an accumulation, throw an error
      end

    """
    # get the current value
    #accum_cnt = self.get_accum_count()
    done = False
    while nowgmt() < self.end_integr:  # test at the usec level
      time.sleep(0.001)
    self.end_integr = nowgmt() + self.integr_time
    return self.get_spectrum()

  def quit(self):
    """
    """
    #if self.parent is not None:
    #  if hasattr(self.parent, "quit"):
    #    self.parent.callback.finished(("file", self.data_file_obj.file.filename,
    #                                  calendar.timegm(time.gmtime())))
    self.logger.info("quit: %s closed.", self.data_file_obj.file.filename)
    #self.data_file_obj.close()

  def help(self):
    return SAOfwif.command_help
    
  # ----------------------- added from hardware ----------------------
  
  def get_firmware_keys(self):
    """
    Sets (and returns) the names of the firmwares available on this host
    """
    self.firmware_keys = self.firmware_server.get_keys()
    self.logger.debug("get_firmware_keys: keys: %s", self.firmware_keys)
    return self.firmware_keys

  def get_params(self, key=None):
    """
    Look up the parameters for the design identified by the key

    Note that is is firmware in dependent in that it just reports on the
    registers of whatever firmware, as long as the firmware is known.

    @param key : name by which the firmware is known
    """
    if key == None:
      key = self.firmware
    self.logger.debug("get_params: invoked for %s", key)
    self.summary = self.firmware_server.firmware_summary(key)
    #self.logger.debug("get_params: summary is %s", self.summary)
    self.bitstream = self.summary['bitstream']
    self.clock = self.summary['clock']
    self.bandwidth = self.summary['bandwidth']
    self.num_chan = self.summary['nchans']
    self.ADC_inputs = self.summary['ADC inputs'] # used by the firmware
    self.ADC_type = self.summary['ADC types']
    self.fft_shift = self.summary['fft_shift']


  class Channel(support.PropertiedClass):
    """
    A logical spectrometer corresponding to one 'polarization' in firmware.

    Polarizations here means independent inputs.  They need not be orthogonal
    polarizations of the same signal.

    One channel corresponds to one RF input, one of several identical signal
    processing paths.  It produces one power and one kurtosis spectrum.

    Public Attributes::
      fpga          - parent.fpga, the FPGA which implements this spectrometr
      freqs         - freqs.freqs
      logger        - logging Logger instance for this class
      name          -
      parent        - Kurt_fwif() instance
      num_chan      - number of channels
      self.parent   - ROACH (SAOfwif object) to which this channel belongs
      pol           - basically the ADC RF input, 0 or 1.
      refpix        - channel number for center frequency
      rf_enabled    - RF status of each ADC
      rf_gain       - RF gain of each ADC channel
      RFnum         -
      roach         - name of the host ROACH
      stats         - signal statistic type (e.g. variance, kurtosis)
    Methods:::
      ADC_samples   -
      get_accums    -
      get_ADC_input -
      get_ADC_snap  -
      get_RF_input  -

    Attributes inherited from PropertiedClass::
      base -
      keys -

    Methods inherited from PropertiedClass::
      has_key -
    """
    def __init__(self, parent, name, active=True):
      """
      Initialize a logical spectrometer

      @param parent : object which created this instance
      @type  parent : KurtosisSpectrometer.DSProc instance
      """
      self.name = name
      self.RFnum = int(name[-1])
      self.parent = parent
      self.logger = logging.getLogger(parent.logger.name+".Channel")
      self.logger.debug(" __init__: for %s", self)
      self.freqs = self.parent.freqs
      self.rf = {}
      self.rf_enabled = True
      self.rf_gain = 0.

    def rf_gain_get(self):
      """
      Get the gain of the RF stage; sets attribute rf_gain
      """
      self.rf = {"enabled": self.rf_enabled, "gain": self.rf_gain}
      self.logger.info("rf_get_gain: %s %s gain[0] = %f, enabled = %s",
                       self.parent.name, self.name, self.rf_gain, 
                       self.rf_enabled)

    def rf_gain_set(self, gain=0):
      """
      Configures the RF attenuators.

      For KATADC boards. KATADC's valid range is -11.5 to 20dB.
      """
      self.logger.debug("rf_gain_set: setting %s RF%d gain to %5.1f",
                        self.name, self.RFnum, gain)
      self.rf_gain = gain
      self.rf['gain'] = gain
      self.rf_gain_get()

    def get_accums(self):
      """
      Get the power and kurtosis from their accumulation registers.
      """
      pass

    def get_ADC_snap(self, now=False):
      """
      Get the contents of the specified ADC snap block. 
      
      This returns a normal distribution of integers with a standard deviation
      of 57, which corresponds to 0 dBm into the ADC chip.

      @param now : True: a snap is triggered.  False: the last data are read.
      """
      self.logger.debug("get_ADC_snap: called for %s %s",self.parent.name, self)
      data = stats.norm.rvs(scale=107/math.sqrt(math.pi), 
                            size=2048)
      self.logger.debug("get_ADC_input: returning %d samples", len(data))
      return numpy.array(data, dtype=numpy.int8)

    def get_ADC_input(self):
      """
      Signal level into the ADC in various units.
      """
      self.logger.debug("get_ADC_input: called for %s RF %d",
                        self.parent.name, self.RFnum)
      samples = self.get_ADC_snap(now=True)
      if samples is None:
        self.logger.warning("get_ADC_input: failed to get samples")
        return None
      else:
        level = {}
        level["sample mean"] = samples.mean()
        level["sample std"]  = samples.std()
        level["Vrms ADC"] = level["sample std"]*ROACH1.adc_cnt_mv_scale_factor()/1000
        level["W ADC"] = RA.volts_to_watts(level["Vrms ADC"])
        level["dBm ADC"] = RA.v_to_dbm(level["Vrms ADC"])
        self.logger.info("get_ADC_input: %s", level)
        return level

    def ADC_samples(self):
      """
      """
      self.logger("ADC_samples: entered")
      return self.get_ADC_snap(now=True)

    def get_RF_input(self):
      """
      Signal level into the KATADC RF section
      """
      level = self.get_ADC_input()
      if level != None:
        # Kurtosis spectrometer has only one ADC in ZDOC 0.
        level["dBm RF"] = level["dBm ADC"] - self.rf_gain
        factor = RA.gain(self.rf_gain)
        level["W RF"] = level["W ADC"]/factor
      return level      


@Pyro5.server.expose
class SAOspecServer(support.pyro.pyro5_server.Pyro5Server, SAObackend):
  """
  Pyro server for the SAO back end

  Public Attributes::
    datafile - file object to which the data are written
    logger   - logging.Logger object
    name     - spectrometer identifier
    run      - True when server is running
  Inherited from SAObackend::
    roach    - back end channels (2 for DTO, 4 for SAO)
  Inherited from Pyroserver::
    logger - logging.Logger object but superceded
    run    - True if server is running
  """
  def __init__(self, name, roachlist=['roach1', 'roach2', 'roach3', 'roach4'], 
                     logpath=support.local_dirs.log_dir, template='roach'):
    """
    Initialize an SAO spectrometer server
    """
    self.name = name
    mylogger = logging.getLogger(logger.name+".SAOspecServer")
    #mylogger.debug("__init__: arg 'name': %s (%s)", name, type(name))
    if roachlist:
      mylogger.debug("__init__: arg 'roachlist': %s", roachlist)
    support.pyro.pyro5_server.Pyro5Server.__init__(self, obj=self, name=name)
    mylogger.debug(" PyroServer superclass initialized")
    #mylogger.debug("__init__: self.name: %s (%s)", self.name, type(self.name))
    # attach the local ROACH boards
    SAObackend.__init__(self, self.name,
                              roachlist=roachlist,
                              template=template,
                              TAMS_logging=False)
    # self.set_integration(int_time) # already done by SAObackend
    self.logger = mylogger
    self.run = True
    self.logger.debug("__init__: done")

  def roaches_available(self):
    """
    """
    return list(self.roach.keys())

  def stop(self):
    """
    Stops the radiometer and closes the datafile
    """
    self.quit() # for the SAObackend
    self.logger.warning("stop: finished.")

  def server_help(self):
    """
    """
    helptext = """
    bandwidth(roach):           returns the spectrometer bandwidth
    check_fans(roach):          check the speed of the chassis fans
    check_temperatures(roach):  returns physical temperatures on the ROACH board
    clock_synth_status(roach):  Returns the status of the sampling clock
    firmware(roach):            returns firmware loaded in specified ROACH
    freqs(roach):               returns the channel frequencies
    get_ADC_input(roach, RF):   returns the power level into the ADC
    read_register(roach, register): returns register contents
    rf_enabled(roach, RF):      returns whether the RF section is enabled or not
    rf_gain_get(roachname, RF): returns the gain of the specified RF channel
    stop():                     Stops the radiometer and closes the datafile
    """
    return helptext

        
def create_arg_parser():
    import argparse
    parser = argparse.ArgumentParser(
                                   description="Fire up APC(A) control server.")
    parser.add_argument("--workstation", "-wsn", dest="wsn", default=0,
                        required=False, action="store",type=int,
             help="Select the operator workstation you'll be connecting to (0)")
    parser.add_argument("--site","-st",dest="site",default="CDSCC",
                        required=False, action="store",
                        help="Select the DSN site you'll be accessing (CDSCC)")
    parser.add_argument("--dss","-d",dest="dss",default=43,required=False,
                        action="store",
               help="Select which DSS antenna the server will connect to. (43)")
    parser.add_argument("--simulated","-s",dest="s",default=False,
                        required=False, action="store_true",
                        help="In simulation mode"+
                  " the APC(A) server won't connect to a control server (True)")
    parser.add_argument("--local","-l",dest="local",default=True,
                        required=False, action="store",
                        help="In local mode, the APC(A) server will fire up"+
                       " on localhost, without creating any SSH tunnels (True)")
    parser.add_argument("--verbose","-v",dest="verbose",default=True,
                        required=False, action="store_true",
                        help="In verbose mode, the log level is DEBUG")

    return parser


def main(server_cls):
    """
    starts logging, creates a server_cls object, launches the server object
    
    This is generic; not specific to the spectrometer server.
    """
    def _main():
        from support.logs import setup_logging
        import datetime
        import os
        
        parsed = create_arg_parser().parse_args()
        
        level = logging.DEBUG
        if not parsed.verbose:
            level = logging.INFO
        # logdir = "/home/ops/roach_data/sao_test_data/logdir/"
        logdir = "/usr/local/Logs/"+socket.gethostname()
        if not os.path.exists(logdir):
            logdir = os.path.dirname(os.path.abspath(__file__))
        timestamp = datetime.datetime.utcnow().strftime("%Y-%j-%Hh%Mm%Ss")
        logfile = os.path.join(
            logdir, "{}_{}.log".format(
                server_cls.__name__, timestamp
            )
        )
        setup_logging(logLevel=level, logfile=None)
        server = server_cls(
           name="test"
        )
        # print(server.feed_change(0, 0, 0))
        server.launch_server(
            ns=False,
            objectId="backend",
            objectPort=50004,
            local=parsed.local,
            threaded=False
        )

    return _main

if __name__ == '__main__':
    main(SAOspecServer)()
