"""
Python support for ROACH1 firmware
"""
import logging
import threading
import time

import Pyro5.api
import Pyro5.errors

import MonitorControl.BackEnds as BE
import MonitorControl.BackEnds.ROACH1.simulator as ROACH1
from support.pyro import asyncio

logger = logging.getLogger(__name__)

max_spectra_per_scan = 120 # 1 h with default 5~s integration
max_num_scans = 12 # 0.5 d with above default


class SAOclient(BE.Backend):
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
        BE.Backend.__init__(self, name, inputs=inputs, output_names=output_names)
        uri = Pyro5.api.URI("PYRO:Spec@localhost:50004")
        if hardware:
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
            self.hardware = ROACH1.SAOspecServer('test')
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
        # callback handler
        self.cb_receiver = asyncio.CallbackReceiver(parent=self)
        self.data_getter = threading.Thread(target=self.get_data, daemon=True)
        self.data_getter.start()
    
    def get_data(self):
        while True:
          data = self.cb_receiver.queue.get()
          print( data[1]['device'], data[1]['time'] )
        self.data_getter.join()
        
    @property
    def scan_finished(self):
        done = [self.scans[name]["done"] for name in self.scans]
        return all(done)

    @Pyro5.api.expose
    @Pyro5.api.callback
    def input_handler(self, res):
        """
        Handles the response from start() which is invoked for each ROACH
        
        The command for starting all the ROACHs is start_recording().
        this is the old way of handling the data returned by the spectrometer
        server
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
                        
    def start_recording(self,
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
                cb=self.cb_receiver
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


