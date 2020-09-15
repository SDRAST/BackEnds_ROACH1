import asyncio
import logging
import Pyro5.api
import Pyro5.errors
import queue
import sys
import threading

logger = logging.getLogger(__name__)

#import MonitorControl.BackEnds.ROACH1.simulator as ROACH1
from support.pyro.asyncio import CallbackReceiver
  

if __name__ == "__main__":
  logging.basicConfig()
  mainlogger = logging.getLogger()
  mainlogger.setLevel(logging.DEBUG)
    
  # be = ROACH1.SAOspecServer('test')
  uri = Pyro5.api.URI("PYRO:backend@localhost:50004")
  hardware = Pyro5.api.Proxy(uri)
  try:
      hardware.__get_state__()
      hardware._pyroRelease()
  except Pyro5.errors.CommunicationError as details:
      mainlogger.error("__init__: %s", details)
      raise Pyro5.errors.CommunicationError("is the SAO spec server running?")
  except AttributeError:
      # no __get_state__ because we have a connection
      pass
  
  cb_receiver = CallbackReceiver()
  hardware.start(n_accums=30, integration_time=10, callback=cb_receiver)
  while True:
    got = cb_receiver.queue.get()
    if got:
      print(got['scan'], got['record'], got['time'])
    else:
      print("empty result")
