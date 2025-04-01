import subprocess
import os
import psutil
import time
import asyncio
from websockets.sync.server import serve

from threading import Thread

computeProc = subprocess.Popen(["./compute/compute"])
isAlive = True

benchProc = None
isBench = False

nodeName = os.environ["NODE_NAME"]

def startHandler():
    global isAlive
    global computeProc
    if not isAlive:
        computeProc = subprocess.Popen(["./compute/compute"])
        isAlive = True
        print("Compute process started.")
    else:
        computeProc.terminate()
        computeProc.wait()
        isAlive = False
        print("Compute process stopped.")

def benchHandler():
    global isBench
    global benchProc
    if not isBench:
        benchProc = subprocess.Popen(["sh", "run_benchmark.sh"])
        isBench = True
        print("Benchmark process started.")
    else:
        benchProc.terminate()
        benchProc.wait()
        isBench = False
        print("Benchmark process stopped.")

def handle_message(websocket, message):
    handlers = {
        "START": startHandler,
        "BENCH": benchHandler,
    }
    hd = handlers.get(message)
    if hd:
        hd()
    else:
        print(f"Unknown message: {message}")

def quit():
    global isAlive
    global computeProc
    global benchProc
    global isBench
    if isAlive:
        computeProc.terminate()
        computeProc.wait()
        isAlive = False
    if isBench:
        benchProc.terminate()
        benchProc.wait()
        isBench = False
    print("Quitting...")
    exit()

def sendUtil(websocket):
    global isAlive
    global isBench
    websocket.send(f"{nodeName}")
    while True:
        cpuUtil = psutil.cpu_percent(interval=1)
        websocket.send(f"{cpuUtil},{int(isAlive)},{int(isBench)}")
        time.sleep(0.5)

def handler(websocket, path):
    global utilThread
    utilThread = Thread(target=sendUtil, args=(websocket,))
    utilThread.start()
    try:
        for message in websocket:
            handle_message(websocket, message)
    except Exception as e:
        print(f"Connection closed: {e}")
        quit()

with serve(handler, "0.0.0.0", 6000) as server:
    server.serve_forever()