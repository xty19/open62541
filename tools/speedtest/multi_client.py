import os
import sys
import subprocess
import threading
import time
from datetime import datetime

client_executable        = "client"
server_address           = "localhost"
port_nr                  = "16664"
namespace_index          = 1
variable_name            = None
client_count             = 1
array_size               = 10
transaction_size         = 1
max_transaction_count    = 1000000
max_measure_duration     = 1 # seconds
runout_duration          = 0.5 # seconds
out_csv                  = "measurements.csv"
max_retries              = 2
verify                   = False
sync_begin               = True
sync_end                 = True
verbose                  = False

args = sys.argv
for i in range(1, len(args)):
  option = args[i]
  if option == "-v":
    verbose = True
  elif option == "-verify":
    verify = True
  elif i + 1 < len(args):
    value = args[i + 1]
    if option == "-client":
      client_executable = value
    elif option == "-server":
      server_address = value
    elif option == "-port":
      port_nr = value
    elif option == "-ns":
      namespace_index = int(value)  
    elif option == "-clients":
      client_count = int(value)
    elif option == "-var":
      variable_name = value  
    elif option == "-size":
      array_size = int(value)
    elif option == "-vars":
      transaction_size = int(value)
    elif option == "-repeat":
      max_transaction_count = int(value)
    elif option == "-duration":
      max_measure_duration = float(value)
    elif option == "-runout":
      runout_duration = float(value)
    elif option == "-csv":
      out_csv = value

args = \
  client_executable + \
  " -server "  + server_address + \
  " -port "    + port_nr + \
  " -ns "      + str(namespace_index) + \
  " -clients " + str(client_count) + \
  " -size "    + str(array_size) + \
  " -vars "    + str(transaction_size) + \
  " -repeat "  + str(max_transaction_count) + \
  " -runout "  + str(runout_duration)
if variable_name is not None:
  args += " -var " + variable_name
if sync_begin:
  args += " -sync_begin"
if sync_end:
  args += " -sync_end"
if verify:
  args += " -verify"

if verbose:
  print(args)

def run(*args, **kwargs):
  subprocess.call(args, **kwargs)

def start_client(args, client_idx, out_file):
    thread = threading.Thread(target = run, args = tuple(args.split()), kwargs = {"stdout": out_file})
    thread.start()
    if verbose:
      print("Client " + str(client_idx) + " started.")
    return thread

measuring        = False
measurement_done = False
retries          = 0
while not measurement_done:

  measure_trigger = "measure"
  if os.path.exists(measure_trigger):
    os.remove(measure_trigger)

  clients = []
  for i in range(client_count):
    if not os.path.exists("logs"):
      os.mkdir("logs")
    log_name = "logs/" + str(i) + ".log"
    log_file = open(log_name, "w")
    clients += [(start_client(args, i, log_file), i, log_name, log_file)]

  time.sleep(0.1 * client_count)
  open(measure_trigger, "w").close()
  measuring = True
  if verbose:
    print("Measuring started.")

  begin_time = datetime.now()
  first_client_duration = None

  transaction_count_sum    = 0
  transaction_count_min    = 1000000000
  transaction_count_max    = 0
  transaction_duration_min = 1e12
  transaction_duration_sum = 0
  transaction_duration_max = 0
  measurement_duration_sum = 0
  measurement_duration_min = 1e12
  measurement_duration_max = 0
  bandwidth_sum            = 0
  bandwidth_min            = 1e12
  bandwidth_max            = 0.0

  measurement_ok = True
  while clients:
    for client in clients:
      thread, i, log_name, log_file = client
      if not thread.is_alive():
        if first_client_duration is None:
          first_client_duration = (datetime.now() - begin_time).total_seconds()

        log_file.close()
        log_file = open(log_name, "r")
        lines = log_file.readlines()
        line = lines[0].strip()
        line_items =line.split()
        try:
          transaction_count, measurement_duration, transaction_duration_items = \
            int(line_items[0]), float(line_items[2]), line_items[10].split("/")
        except:
          transaction_count = measurement_duration = 0
          transaction_duration_items = [0, 0, 0]

        if transaction_count == 0 or measurement_duration == 0:
          print("ERROR: Client " + str(i) + " transactions failed: " + line)
          measurement_ok = False

        array_item_size = 4 # Int32
        if measurement_duration > 0:
          bandwidth = array_size * array_item_size * transaction_size * transaction_count / measurement_duration
        else:
          bandwidth = 0

        transaction_duration_sum += float(transaction_duration_items[1])
        transaction_duration_min = min(transaction_duration_min, float(transaction_duration_items[0]))
        transaction_duration_max = max(transaction_duration_max, float(transaction_duration_items[2]))
        measurement_duration_sum += measurement_duration
        measurement_duration_min = min(measurement_duration_min, measurement_duration)
        measurement_duration_max = max(measurement_duration_max, measurement_duration)
        transaction_count_sum   += transaction_count
        transaction_count_min    = min(transaction_count_min, transaction_count)
        transaction_count_max    = max(transaction_count_max, transaction_count)
        bandwidth_sum           += bandwidth
        bandwidth_min            = min(bandwidth_min, bandwidth)
        bandwidth_max            = max(bandwidth_max, bandwidth)

        if verbose:
          print("Client " + str(i) + " done: " + line)

        clients.remove(client)
        break

    if sync_end and measuring and (datetime.now() - begin_time).total_seconds() > max_measure_duration:
      while True:
        # noinspection PyBroadException
        try:
          os.remove(measure_trigger)
          measuring = False
          break
        except:
          assert((datetime.now() - begin_time).total_seconds() < max_measure_duration + 2)
      if verbose:
        print("Measuring done. Runout ...")

    time.sleep(0.01)

  if verbose:
    print("All done.")

  if client_count > 1 and measurement_duration_max > first_client_duration:
    print("ERROR: Run-out too short. "
          "Max measurement duration: " + str(measurement_duration_max) + " sec, "
          "First client duration: " + str(first_client_duration) + " sec")
    measurement_ok = False

  if measurement_ok:
    measurement_done = True
  else:
    if retries == max_retries:
      break
    print("Retrying ...")
    retries += 1
    runout_duration *= 2

transaction_bytes        = array_size * array_item_size * transaction_size
transaction_count_avg    = transaction_count_sum / client_count
transaction_duration_avg = transaction_duration_sum / client_count
measurement_duration_avg = measurement_duration_sum / client_count
bandwidth_avg            = bandwidth_sum / client_count
if measurement_duration_max > 0:
  bandwidth_sum_alt1 = client_count * transaction_bytes * transaction_count_avg / measurement_duration_avg
  bandwidth_sum_alt2 = client_count * transaction_bytes * transaction_count_avg / measurement_duration_max
  bandwidth_sum_alt3 = transaction_bytes * transaction_count_sum / measurement_duration_max

if not measurement_ok or measurement_duration_max == 0:
  bandwidth_sum      = 0
  bandwidth_sum_alt1 = 0
  bandwidth_sum_alt2 = 0
  bandwidth_sum_alt3 = 0

out_keys = ["client_executable", "client_count", "array_size", "transaction_size", "transaction_bytes",
            "measurement_duration_min", "measurement_duration_avg", "measurement_duration_max",
            "transaction_count_min", "transaction_count_avg", "transaction_count_max",
            "transaction_duration_min", "transaction_duration_avg", "transaction_duration_max",
            "bandwidth_min", "bandwidth_avg", "bandwidth_max",
            "bandwidth_sum", "bandwidth_sum_alt1", "bandwidth_sum_alt2", "bandwidth_sum_alt3"]
out_data = {key: globals()[key] for key in out_keys}

s = ""
for key in out_keys:
  s += key + ": " + str(out_data[key]) + ", "
print(s)

if transaction_count > 0:
  csv = open(out_csv, "a")
  for key in out_keys:
    csv.write(str(out_data[key]) + ",")
  csv.write("\n")
  csv.close()
