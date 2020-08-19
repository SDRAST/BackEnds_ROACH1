import asyncio
import logging
import Pyro5.api
import queue
import sys
import threading

logger = logging.getLogger(__name__)

import MonitorControl.BackEnds.ROACH1.simulator as ROACH1
from support.pyro.asyncio import CallbackReceiver, CallbackQueue
  

if __name__ == "__main__":
  logging.basicConfig()
  mainlogger = logging.getLogger()
  mainlogger.setLevel(logging.DEBUG)
    
  be = ROACH1.SAOspecServer('test')
  cb_q = CallbackQueue()
  cb_receiver = CallbackReceiver(cb_q)
  be.start(n_accums=3, integration_time=3, callback=cb_receiver)

