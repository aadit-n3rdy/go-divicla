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

def start():
    global isAlive
    global computeProc
    if not isAlive:
        computeProc = subprocess.Popen(["./compute/compute"])
        isAlive = True
        print("Compute process started.")
    else:
        print("Compute process is already running.")

def stop():
    global isAlive
    global computeProc
    if isAlive:
        computeProc.terminate()
        computeProc.wait()
        isAlive = False
        print("Compute process stopped.")
    else:
        print("Compute process is not running.")

def start_bench():
    global isBench
    global benchProc
    if not isBench:
        benchProc = subprocess.Popen(["sh", "run_benchmark.sh"])
        isBench = True
        print("Benchmark process started.")
    else:
        print("Benchmark process is already running.")

def stop_bench():
    global isBench
    global benchProc
    if isBench:
        benchProc.terminate()
        benchProc.wait()
        isBench = False
        print("Benchmark process stopped.")
    else:
        print("Benchmark process is not running.")

def on_message(ws, message):
    handlers = {
        "START": start,
        "STOP": stop,
        "BENCH_START": start_bench,
        "BENCH_STOP": stop_bench,
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
    stop_bench()
    stop()
    exit()

def sendUtil():
    global ws
    global isAlive
    global isBench
    while True:
        cpuUtil = psutil.cpu_percent(interval=1)
        ws.send(f"{cpuUtil},{isAlive},{isBench}")
        time.sleep(0.5)

ws = websocket.WebsocketApp("wss://" + demoUrl + f"/{nodeName}", on_message = on_message, on_error = on_error, on_close = on_close)

utilThread = Thread(target=sendUtil, daemon=True).start()

ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})  

