from pylab import *

df = open("servertest.data", "r")
lines = df.readlines()
endtime = {}
entered = {}
released = {}
acquired = {}
passed = {}
stored = {}
for line in lines:
  linedata = line.strip().split()
  if 'roach' in linedata[1]:
    roach = linedata[1]
    if linedata[3] == "stop":
      endtime[roach] = float(linedata[5])
    if 'action' in linedata[0]:
      if len(linedata) == 6 and linedata[5] == "entered":
        if roach in entered:
          pass
        else:
          entered[roach] = {}
        record = int(linedata[4])
        time = float(linedata[3])
        entered[roach][record] = time
      elif len(linedata) == 7 and linedata[4] == "got":
        if roach in released:
          pass
        else:
          released[roach] = {}
        record = int(linedata[6])
        time = float(linedata[3])
        released[roach][record] = time
      elif len(linedata) == 6 and linedata[4] == "finished":
        if roach in acquired:
          pass
        else:
          acquired[roach] = {}
        record = int(linedata[5])
        time = float(linedata[3])
        acquired[roach][record] = time
    elif 'combine' in linedata[0]:
      if linedata[-1] == "entered":
        if roach in passed:
          pass
        else:
          passed[roach] = {}

        record = int(linedata[-2])
        time = float(linedata[-3])
        passed[roach][record] = time
      elif linedata[-2] == "stored":
        if roach in stored:
          pass
        else:
          stored[roach] = {}
        record = int(linedata[-1])
        time = float(linedata[-3])
        stored[roach][record] = time

# integration times
figure()       
for roach in released:
  integration = []
  for rec in released[roach]:
    integration.append(released[roach][rec]-entered[roach][rec])
  hist(integration, histtype='step', label=roach)
  legend()
  title('Integration Times')
  xlabel('Seconds')
  
# reading times
figure()       
for roach in released:
  reading = []
  for rec in released[roach]:
    reading.append(acquired[roach][rec]-released[roach][rec])
  hist(reading, histtype='step', label=roach)
  legend()
  title('Reading Times')
  xlabel('Seconds')

# combiner queue time
figure()
for roach in acquired:
  queuetime = []
  for rec in acquired[roach]:
    queuetime.append(passed[roach][rec]-acquired[roach][rec])
  hist(queuetime, histtype='step', label=roach)
  legend()
  title('Queue Times')
  xlabel('Seconds')

# combiner times
figure()
for roach in passed:
  combinetime = []
  for rec in stored[roach]:
    combinetime.append(stored[roach][rec]-passed[roach][rec])
  hist(combinetime, histtype='step', label=roach)
  legend()
  title("Combiner Times")
  xlabel("Seconds")
show()  
