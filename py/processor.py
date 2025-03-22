import sys

print("Hello candle!")
sys.stdout.flush()

import torch

print("Candle got torch") 
sys.stdout.flush()

import socket
import os
import struct

print("Loading model")
sys.stdout.flush()

model = torch.jit.load("./models/model.pt")

print("Loaded model")
sys.stdout.flush()

procport = int(os.getenv("CANDLE_PORT", "5678"))

servsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
servsock.bind(("", procport))

servsock.listen(5)
print("Candle listening on port", procport)
sys.stdout.flush()

while True:
    sock, addr = servsock.accept()
    print("Accepted from addr")
    sys.stdout.flush()
    stream = sock.makefile("rwb")
    print("Started")
    sys.stdout.flush()
    while True:
        print("CANDLE Reading line", flush=True)
        ln = stream.readline()
        print("candle Size line: ", ln, flush=True)
        sizes = [int(x) for x in ln.decode().strip().split(" ")]
        print("candle sizes: ", sizes, flush=True)
        count = 1
        for s in sizes:
            count *= s
        inp = torch.zeros(count)
        print("CANDLE Reading values", flush=True)
        for i in range(0, count):
            buf = []
            for j in range(0, 4):
                tmp = stream.read(1)
                while len(tmp) < 1:
                    tmp = stream.read(1)
                buf.extend(tmp)
            (val,) = struct.unpack("f", bytearray(buf))
            inp[i] = val
        print("CANDLE read values", flush=True)
        inp = inp.reshape(sizes)
        print("CANDLE running model", flush=True)
        op = model(inp)
        print("CANDLE result: ", op, flush=True)
        val1 = op[0][0].item()
        val2 = op[0][1].item()
        stream.write(f"{val1} {val2}\n".encode())
        stream.flush()
        print("CANDLE wrote results")
        print("CANDLE op: ", op, flush=True)
    print("Stopped")
