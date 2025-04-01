import subprocess
import os
import psutil
import time
import asyncio
import websockets

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

async def handle_message(websocket, message):
    handlers = {
        "START": startHandler,
        "BENCH": benchHandler,
    }
    hd = handlers.get(message)
    if hd:
        hd()
    else:
        print(f"Unknown message: {message}")

async def quit():
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

async def sendUtil(websocket):
    global isAlive
    global isBench
    await websocket.send(f"{nodeName}")
    while True:
        cpuUtil = psutil.cpu_percent(interval=1)
        await websocket.send(f"{cpuUtil},{int(isAlive)},{int(isBench)}")
        await asyncio.sleep(0.5)

async def handler(websocket, path):
    global utilThread
    utilThread = Thread(target=lambda: asyncio.run(sendUtil(websocket)), daemon=True)
    utilThread.start()
    try:
        async for message in websocket:
            await handle_message(websocket, message)
    except websockets.exceptions.ConnectionClosed as e:
        print(f"Connection closed: {e}")
        await quit()

start_server = websockets.serve(handler, "0.0.0.0", 6000)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
