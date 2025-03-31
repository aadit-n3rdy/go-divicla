import subprocess
import os
import websocket, ssl
import psutil
import time

from threading import Thread

computeProc = subprocess.Popen(["./compute/compute"])
isAlive = True

benchProc = None
isBench = False

demoUrl = os.environ["DEMO_URL"]
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

def on_message(ws, message):
    handlers = {
        "START": startHandler,
        "BENCH": benchHandler,
    }
    hd = handlers.get(message)
    if hd:
        hd()
    else:
        print(f"Unknown message: {message}")

def on_error(ws, error):
    print(f"Error: {error}")
    quit()

def on_close(ws):
    print("WebSocket closed")
    quit()

def quit():
    global utilThread
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

def sendUtil():
    global ws
    global isAlive
    global isBench
    ws.send(f"{nodeName}")
    while True:
        cpuUtil = psutil.cpu_percent(interval=1)
        ws.send(f"{cpuUtil},{int(isAlive)},{int(isBench)}")
        time.sleep(0.5)

ws = websocket.WebsocketApp(demoUrl, on_message = on_message, on_error = on_error, on_close = on_close)

utilThread = Thread(target=sendUtil, daemon=True).start()

ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})  

