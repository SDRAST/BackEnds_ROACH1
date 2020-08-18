import asyncio
import logging
import Pyro5.api
import queue
import sys
import threading

logger = logging.getLogger(__name__)

import MonitorControl.BackEnds.ROACH1.simulator as ROACH1

class CallbackReceiver(object):
  """
  """
  def __init__(self, queue):
    """
    """
    self.logger = logging.getLogger(logger.name+".CallbackReceiver")
    self.queue = queue
    self.daemon = Pyro5.api.Daemon()
    self.uri = self.daemon.register(self)
    self.lock = threading.Lock()
    with self.lock:
            self._running = True
  
    self.thread = threading.Thread(target=self.daemon.requestLoop, 
                                   args=(self.running,) )
    self.thread.daemon = True
    self.thread.start()
    #self.thread.join()

  @Pyro5.api.expose
  def running(self):
    """
    Get running status of server
    """
    with self.lock:
      return self._running
  
  @Pyro5.api.expose
  @Pyro5.api.callback
  def finished(self, *args):
    """
    Note that code is executed by the remote server
    """
    print("got %d items from remote; do 'cb_q.get()' to get them"
          % len(args))
    self.queue.put(args)
    self.logger.debug("finished: put on queue: %s", args)
    # at this point another thread processes the data

class CallbackQueue(queue.Queue):
  """
  """
  def __init__(self):
    #super(CallbackQueue, self).__init__(self) # causes a problem with 'put'
    queue.Queue.__init__(self)
  

if __name__ == "__main__":
  logging.basicConfig()
  mainlogger = logging.getLogger()
  mainlogger.setLevel(logging.DEBUG)
    
  be = ROACH1.SAOspecServer('test')
  cb_q = CallbackQueue()
  cb_receiver = CallbackReceiver(cb_q)
  be.start(n_accums=3, integration_time=3, callback=cb_receiver)

