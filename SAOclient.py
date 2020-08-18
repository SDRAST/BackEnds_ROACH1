"""
Python support for ROACH1 firmware
"""
import logging
import time

import Pyro5.api
import Pyro5.errors

import MonitorControl.BackEnds as BE
from support.pyro import asyncio

logger = logging.getLogger(__name__)

max_spectra_per_scan = 120 # 1 h
max_num_scans = 120 # 5 d


class SAOclient(BE.Backend, asyncio.CallbackReceiver):
    """
    SAO 32K-channel spectrometer

    consists of four ROACH boards, each running the same firmware. It takes in
    four IF class signals and outputs digital data (spectra) for each channel.

    The details of this class depend on the SAO spectrometer firmware in the
    ROACH processors.

    This is a client class which uses the server controlling the spectrometer.
    """
    num_chan = 32768

    def __init__(self, name, inputs=None, output_names=None, hardware=None):
        """
        create an SAOspec instance

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
        uri = Pyro5.api.URI("PYRO:Spec@localhost:50004")
        if hardware:
            self.hardware = async.AsyncProxy(uri)
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
        self.name = name
        self.logger = mylogger
        self.logger.info("__init__: %s input channels: %s", self, self.inputs)
        self.update_signals() # from Device super class
        scans = {}
        if self.hardware:
            scans = {name: {
                        "done":False,
                        "scan":0
                     }
                     for name in self.hardware.roachnames}
        self.scans = scans
        # callback handler and queue
        self.cb_q = CallbackQueue()
        self.cb_receiver = CallbackReceiver(cb_q)
    @property
    def scan_finished(self):
        done = [self.scans[name]["done"] for name in self.scans]
        return all(done)

    @Pyro5.api.expose
    @Pyro5.api.callback
    def start_handler(self, res):
        """
        this is the old way o
        """
        if hasattr(res, "count"):  # means we got a tuple or a list
            status, name, scan, finish_time = res
            self.scans[name]["scan"] = scan
            if status == "done":
                self.scans[name]["done"] = True
            elif status == "record":
                self.scans[name]["done"] = False
        else:
            raise TypeError(
                ("start_handler: Can't process {} "
                 "responses from start hardware method").format(
                    type(res)
                 ))

    def _reset_scans(self):
        self.scans = {name: {
                        "done":False,
                        "scan":0
                      }
                      for name in self.scans}

    #def __getattr__(self, attr):
        #if self.hardware is not None:
            #return getattr(self.hardware, attr)
        #    return getattr(self, attr)
    #    return getattr(self, attr)

    def start_recording(self,
                        n_scans=max_num_scans,
                        n_accums=max_spectra_per_scan,
                        integration_time=5.0):
        self.logger.debug("start_recording: {} scans".format(n_scans))
        
                        
    def old_start_recording(self,
                        n_scans=max_num_scans,
                        n_accums=max_spectra_per_scan,
                        integration_time=5.0):
        """
        start a series of scans
        """
        self.logger.debug("start_recording: {} scans".format(n_scans))
        for scan in range(n_scans):
            self.start(
                n_accums=n_accums,
                integration_time=integration_time,
                cb=self.start_handler
          )

    def help(self, kind=None):
        """
        """
        if self.hardware is not None:
            if kind == "server":
                return self.hardware.server_help()
            elif kind == "backend":
                return self.hardware.backend_help()
        return "Types available: server, backend"

  # ------------------------ methods for individual ROACHs---------------------

    """
    def auto_gain(self, roachname, best=0):
        pwr = self.hardware.get_ADC_input(roachname)['dBm ADC']
        while best-0.5 > pwr or pwr > best+0.5:
            new_gain = best - pwr
            if new_gain >= 20:
                self.hardware.rf_gain_set(roachname, gain=20)
                return 20
            elif new_gain <= -11.5:
                self.hardware.rf_gain_set(roachname, gain=-11.5)
                return -11.5
            else:
                self.hardware.rf_gain_set(roachname, gain=new_gain)
            pwr = self.hardware.get_ADC_input(roachname)['dBm ADC']
        return self.hardware.rf_gain_get(roachname)
    """